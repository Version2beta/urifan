package urifan

import (
	"context"
	"sync"

	uri "github.com/Version2beta/urifan/transport"
)

type destination struct {
	relay       *Relay
	id          string
	endpoint    Endpoint
	out         *uri.Output
	ctx         context.Context
	cancel      context.CancelFunc
	ownerCancel context.CancelFunc
	queue       chan *packet
	closeOnce   sync.Once
	mu          sync.Mutex
	outstanding map[*packet]int
	count       int
	bytes       int64
	packetLimit int
	byteLimit   int64
	datagram    bool
}

func newDestination(r *Relay, id string, e Endpoint, out *uri.Output, owner context.CancelFunc) *destination {
	ctx, cancel := context.WithCancel(r.ctx)
	packets := e.QueuePackets
	if packets == 0 {
		packets = r.config.QueuePackets
	}
	bytes := e.QueueBytes
	if bytes == 0 {
		bytes = r.config.QueueBytes
	}
	return &destination{relay: r, id: id, endpoint: e, out: out, ctx: ctx, cancel: cancel, ownerCancel: owner, queue: make(chan *packet, packets), outstanding: make(map[*packet]int), packetLimit: packets, byteLimit: bytes, datagram: endpointScheme(e) == "udp"}
}

// enqueue never waits for capacity. Counts include the relay queue, Send's
// current item, urihandler's queue and in-flight write until its release callback.
func (d *destination) enqueue(p *packet) error {
	if d.endpoint.PacketSize > 0 && len(p.data) > d.endpoint.PacketSize {
		return ErrPacketTooLarge
	}
	if d.datagram && len(p.data) > 65507 {
		return ErrPacketTooLarge
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.count >= d.packetLimit || int64(len(p.data)) > d.byteLimit-d.bytes {
		return ErrBacklog
	}
	p.refs.Add(1)
	d.count++
	d.bytes += int64(len(p.data))
	d.outstanding[p]++
	// The outstanding-count limit guarantees a channel slot. Only the actor sends.
	d.queue <- p
	return nil
}
func (d *destination) release(p *packet) {
	d.mu.Lock()
	d.count--
	d.bytes -= int64(len(p.data))
	d.outstanding[p]--
	if d.outstanding[p] == 0 {
		delete(d.outstanding, p)
	}
	d.mu.Unlock()
	p.release()
}
func (d *destination) oldest() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	var n uint64
	for p := range d.outstanding {
		if n == 0 || p.sequence < n {
			n = p.sequence
		}
	}
	return n
}
func (d *destination) finish() { d.closeOnce.Do(func() { close(d.queue) }) }
func (d *destination) abort() {
	d.cancel()
	d.out.Cancel()
	d.finish()
	// Discard queued work immediately. A worker may concurrently receive its own
	// item; each channel receive has unique custody. In-flight leases stay alive.
	for p := range d.queue {
		d.release(p)
	}
}
func (r *Relay) startDestination(d *destination) {
	r.spawn(func() {
		defer d.cancel()
		if d.ownerCancel != nil {
			defer d.ownerCancel()
		}
		var sendErr error
	loop:
		for {
			var p *packet
			select {
			case <-d.out.Done():
				sendErr = d.out.Wait(context.Background())
				if sendErr == nil {
					sendErr = uri.ErrClosed
				}
				break loop
			case next, ok := <-d.queue:
				if !ok {
					break loop
				}
				p = next
			}
			if d.ctx.Err() != nil {
				d.release(p)
				continue
			}
			b := uri.NewBuffer(p.data, func() { d.release(p) })
			if err := d.out.Send(d.ctx, b); err != nil {
				b.Release()
				sendErr = err
				d.out.Cancel()
				break
			}
		}
		if sendErr != nil {
			// Notify the actor before draining: it closes the queue and stops admission.
			select {
			case r.messages <- message{kind: "destination_error", id: d.id, err: sendErr}:
			case <-r.ctx.Done():
			}
			d.cancel()
			d.out.Cancel()
			d.finish()
			for p := range d.queue {
				d.release(p)
			}
		} else if d.ctx.Err() != nil {
			d.out.Cancel()
		} else {
			d.out.Close()
		}
		err := d.out.Wait(context.Background())
		select {
		case r.messages <- message{kind: "destination_end", id: d.id, err: err}:
		case <-r.ctx.Done():
		}
	})
}

package urifan

import (
	"context"
	"sync/atomic"
	"time"

	uri "github.com/Version2beta/urifan/transport"
)

type packet struct {
	data     []byte
	root     *uri.Buffer
	charge   int64
	refs     atomic.Int64
	sequence uint64
	relay    *Relay
}

func (p *packet) release() {
	if p.refs.Add(-1) == 0 {
		p.root.Release()
		p.relay.counters.memory.Add(-p.charge)
		select {
		case p.relay.released <- struct{}{}:
		default:
		}
	}
}
func (r *Relay) reserve(n int64) error {
	if n > r.config.MaxMemoryBytes {
		return ErrMemoryLimit
	}
	reclaimDeadline := time.Now().Add(50 * time.Millisecond)
	for r.counters.memory.Load() > r.config.MaxMemoryBytes-n {
		var oldest *destination
		var seq uint64
		for _, d := range r.destinations {
			v := d.oldest()
			if v != 0 && (oldest == nil || v < seq || (v == seq && d.id < oldest.id)) {
				oldest = d
				seq = v
			}
		}
		if oldest == nil {
			remaining := time.Until(reclaimDeadline)
			if remaining <= 0 || len(r.all) == 0 {
				return ErrMemoryLimit
			}
			timer := time.NewTimer(remaining)
			select {
			case <-r.released:
				timer.Stop()
				continue
			case <-timer.C:
				return ErrMemoryLimit
			case <-r.ctx.Done():
				timer.Stop()
				return context.Cause(r.ctx)
			}
		}
		r.lose(oldest, ErrMemoryLimit)
		// Give canceled in-flight I/O a bounded opportunity to retire its leases.
		// If it cannot, stop safely rather than recycle storage still in use.
		timer := time.NewTimer(time.Until(reclaimDeadline))
	wait:
		for r.counters.memory.Load() > r.config.MaxMemoryBytes-n {
			select {
			case <-r.released:
				if oldest.oldest() == 0 {
					break wait
				}
			case <-timer.C:
				break wait
			case <-r.ctx.Done():
				timer.Stop()
				return context.Cause(r.ctx)
			}
		}
		timer.Stop()
	}
	current := r.counters.memory.Add(n)
	for {
		old := r.counters.peak.Load()
		if current <= old || r.counters.peak.CompareAndSwap(old, current) {
			break
		}
	}
	return nil
}
func (r *Relay) newPacket(n int) (*packet, error) {
	if err := context.Cause(r.ctx); err != nil {
		return nil, err
	}
	if err := r.reserve(int64(n)); err != nil {
		return nil, err
	}
	var root *uri.Buffer
	var err error
	if r.config.Allocator == nil {
		root = uri.NewBuffer(make([]byte, n), nil)
	} else {
		root, err = r.config.Allocator(r.ctx, n)
	}
	if err == nil && r.ctx.Err() != nil {
		err = context.Cause(r.ctx)
	}
	if err != nil || root == nil || cap(root.Data) < n {
		if root != nil {
			root.Release()
		}
		r.counters.memory.Add(-int64(n))
		if err != nil {
			return nil, err
		}
		return nil, uri.ErrInvalidBuffer
	}
	charge := int64(cap(root.Data))
	if charge > int64(n) {
		if err := r.reserve(charge - int64(n)); err != nil {
			root.Release()
			r.counters.memory.Add(-int64(n))
			return nil, err
		}
	}
	root.Data = root.Data[:n]
	r.sequence++
	p := &packet{data: root.Data, root: root, charge: charge, sequence: r.sequence, relay: r}
	p.refs.Store(1)
	return p, nil
}
func (r *Relay) targets() []*destination {
	v := make([]*destination, 0, len(r.destinations))
	for _, d := range r.destinations {
		v = append(v, d)
	}
	return v
}
func (r *Relay) dispatch(p *packet, targets []*destination) {
	r.counters.packets.Add(1)
	for _, d := range targets {
		if r.destinations[d.id] != d {
			continue
		}
		if err := d.enqueue(p); err != nil {
			r.lose(d, err)
		}
	}
	p.release()
}
func (r *Relay) datagram(b []byte) error {
	if len(r.destinations) == 0 {
		return nil
	}
	targets := r.targets()
	p, err := r.newPacket(len(b))
	if err != nil {
		return err
	}
	copy(p.data, b)
	r.dispatch(p, targets)
	return nil
}
func (r *Relay) stream(b []byte) error {
	for len(b) > 0 {
		if err := context.Cause(r.ctx); err != nil {
			return err
		}
		if r.assembly == nil {
			if len(r.destinations) == 0 {
				return nil
			}
			targets := r.targets()
			p, err := r.newPacket(r.config.PacketSize)
			if err != nil {
				return err
			}
			r.assembly = p
			r.recipients = targets
			r.used = 0
			if r.timer == nil {
				r.timer = time.NewTimer(r.config.FlushInterval)
			} else {
				r.timer.Reset(r.config.FlushInterval)
			}
			r.tick = r.timer.C
		}
		n := copy(r.assembly.data[r.used:], b)
		r.used += n
		b = b[n:]
		if r.used == r.config.PacketSize {
			r.flush()
		}
	}
	return nil
}
func (r *Relay) flush() {
	if r.assembly == nil {
		return
	}
	p := r.assembly
	targets := r.recipients
	p.data = p.data[:r.used]
	r.assembly = nil
	r.recipients = nil
	r.used = 0
	r.tick = nil
	if r.timer != nil {
		r.timer.Stop()
	}
	r.dispatch(p, targets)
}
func (r *Relay) discardAssembly() {
	if r.assembly != nil {
		r.assembly.release()
		r.assembly = nil
	}
	r.recipients = nil
	r.used = 0
	r.tick = nil
	if r.timer != nil {
		r.timer.Stop()
	}
}

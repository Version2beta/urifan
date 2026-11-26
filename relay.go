package urifan

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	uri "github.com/Version2beta/urifan/transport"
)

// Event is diagnostic, not an acknowledgement of remote delivery. Error is nil
// for normal transitions. Addresses are local endpoint addresses.
type Event struct {
	Kind, ID, URI, Address string
	Error                  error
}
type Stats struct {
	SourceBytes, Packets                                 uint64
	ActiveDestinations, RetainedBytes, PeakRetainedBytes int64
	LostEvents                                           uint64
}
type counters struct {
	sourceBytes, packets, lost atomic.Uint64
	active, memory, peak       atomic.Int64
}
type message struct {
	kind   string
	e      Endpoint
	in     *uri.Input
	out    *uri.Output
	buffer *uri.Buffer
	err    error
	id     string
	cancel context.CancelFunc
	reply  chan bool
}

// Relay owns all opened endpoints. Close requests draining; Cancel aborts.
// Wait observes actual cleanup and may be bounded by the caller's context.
type Relay struct {
	config         Config
	ctx            context.Context
	cancel         context.CancelCauseFunc
	stopCtx        context.Context
	stopCancel     context.CancelFunc
	closeOnce      sync.Once
	closeRequest   chan struct{}
	stopCloseWatch chan struct{}
	closeWatchDone chan struct{}
	done           chan struct{}
	events         chan Event
	messages       chan message
	released       chan struct{}
	result         error
	addresses      map[string]string
	counters       counters
	wg             sync.WaitGroup
	listeners      []*uri.Listener
	pending        map[string]context.CancelFunc
	destinations   map[string]*destination
	all            map[string]*destination
	activeInput    *uri.Input
	nextID         uint64
	sequence       uint64
	assembly       *packet
	used           int
	recipients     []*destination
	timer          *time.Timer
	tick           <-chan time.Time
	stopping       bool
	sourceDatagram bool
	failed         bool
}

func Start(parent context.Context, c Config) (*Relay, error) {
	c, err := normalize(c)
	if err != nil {
		return nil, err
	}
	if err = parent.Err(); err != nil {
		return nil, err
	}
	if err = preflight(c); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(parent)
	stop, stopCancel := context.WithCancel(ctx)
	r := &Relay{config: c, sourceDatagram: endpointScheme(c.Source) == "udp", ctx: ctx, cancel: cancel, stopCtx: stop, stopCancel: stopCancel, closeRequest: make(chan struct{}), stopCloseWatch: make(chan struct{}), closeWatchDone: make(chan struct{}), done: make(chan struct{}), events: make(chan Event, c.EventCapacity), messages: make(chan message), released: make(chan struct{}, 1), addresses: make(map[string]string), pending: make(map[string]context.CancelFunc), destinations: make(map[string]*destination), all: make(map[string]*destination)}
	cleanup := func(e error) (*Relay, error) {
		stopCancel()
		cancel(e)
		for _, l := range r.listeners {
			l.Close()
		}
		for _, d := range r.destinations {
			d.out.Cancel()
			d.out.Wait(context.Background())
		}
		if r.activeInput != nil {
			r.activeInput.Cancel()
			r.activeInput.Wait(context.Background())
		}
		for _, l := range r.listeners {
			l.Wait(context.Background())
		}
		return nil, e
	}
	type acceptor struct {
		e Endpoint
		l *uri.Listener
	}
	var sinks []acceptor
	var fifo []Endpoint
	// Prepare outputs before enabling any source consumption.
	for _, e := range c.Destinations {
		if e.Listen {
			l, err := uri.Listen(ctx, e.URI, transportOptions(c, e))
			if err != nil {
				return cleanup(fmt.Errorf("destination %s: %w", e.ID, err))
			}
			r.listeners = append(r.listeners, l)
			r.addresses[e.ID] = l.Addr().String()
			r.emit(Event{Kind: "destination_listening", ID: e.ID, URI: e.URI, Address: l.Addr().String()})
			sinks = append(sinks, acceptor{e, l})
		} else if endpointScheme(e) == "fifo" {
			fifo = append(fifo, e)
		} else {
			out, err := uri.OpenOutput(ctx, e.URI, transportOptions(c, e))
			if err != nil {
				return cleanup(fmt.Errorf("destination %s: %w", e.ID, err))
			}
			if out.Addr() != nil {
				r.addresses[e.ID] = out.Addr().String()
			}
			r.install(e, out, nil, false)
		}
	}
	var sourceListener *uri.Listener
	if c.Source.Listen && endpointScheme(c.Source) != "udp" {
		sourceListener, err = uri.Listen(ctx, c.Source.URI, transportOptions(c, c.Source))
		if err != nil {
			return cleanup(fmt.Errorf("source: %w", err))
		}
		r.listeners = append(r.listeners, sourceListener)
		r.addresses[c.Source.ID] = sourceListener.Addr().String()
		r.emit(Event{Kind: "source_listening", ID: c.Source.ID, URI: c.Source.URI, Address: sourceListener.Addr().String()})
	} else {
		r.activeInput, err = uri.OpenInput(ctx, c.Source.URI, transportOptions(c, c.Source))
		if err != nil {
			return cleanup(fmt.Errorf("source: %w", err))
		}
		if r.activeInput.Addr() != nil {
			r.addresses[c.Source.ID] = r.activeInput.Addr().String()
		}
	}
	for _, d := range r.destinations {
		r.startDestination(d)
	}
	for _, a := range sinks {
		a := a
		r.spawn(func() { r.acceptDestinations(a.e, a.l) })
	}
	for _, e := range fifo {
		r.prepareFIFO(e)
	}
	if sourceListener != nil {
		r.spawn(func() { r.acceptSources(sourceListener) })
	} else {
		in := r.activeInput
		r.emit(Event{Kind: "source_connected", ID: c.Source.ID, URI: c.Source.URI, Address: r.addresses[c.Source.ID]})
		r.spawn(func() { r.readInput(in) })
	}
	go r.watchClose()
	go r.run()
	return r, nil
}

// Close's deadline has an independent owner so a context-aware allocator cannot
// prevent the actor from reaching the cancellation that unblocks that allocator.
func (r *Relay) watchClose() {
	defer close(r.closeWatchDone)
	select {
	case <-r.stopCloseWatch:
		return
	case <-r.closeRequest:
	}
	r.stopCancel()
	timer := time.NewTimer(r.config.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-r.stopCloseWatch:
		return
	case <-timer.C:
		r.cancel(ErrShutdownTimeout)
	}
}
func (r *Relay) spawn(f func())        { r.wg.Add(1); go func() { defer r.wg.Done(); f() }() }
func (r *Relay) Close() error          { r.closeOnce.Do(func() { close(r.closeRequest) }); return nil }
func (r *Relay) Cancel()               { r.cancel(context.Canceled) }
func (r *Relay) Done() <-chan struct{} { return r.done }
func (r *Relay) Events() <-chan Event  { return r.events }
func (r *Relay) Addresses() map[string]string {
	m := make(map[string]string, len(r.addresses))
	for k, v := range r.addresses {
		m[k] = v
	}
	return m
}
func (r *Relay) Stats() Stats {
	return Stats{SourceBytes: r.counters.sourceBytes.Load(), Packets: r.counters.packets.Load(), ActiveDestinations: r.counters.active.Load(), RetainedBytes: r.counters.memory.Load(), PeakRetainedBytes: r.counters.peak.Load(), LostEvents: r.counters.lost.Load()}
}
func (r *Relay) Wait(ctx context.Context) error {
	select {
	case <-r.done:
		return r.result
	default:
	}
	select {
	case <-r.done:
		return r.result
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *Relay) emit(e Event) {
	select {
	case r.events <- e:
	default:
		r.counters.lost.Add(1)
	}
}
func (r *Relay) send(m message) bool {
	select {
	case r.messages <- m:
		return true
	case <-r.stopCtx.Done():
		return false
	}
}
func (r *Relay) readInput(in *uri.Input) {
	defer func() { in.Close(); in.Wait(context.Background()) }()
	for {
		select {
		case <-r.stopCtx.Done():
			return
		case b, ok := <-in.Data():
			if !ok {
				err := in.Wait(context.Background())
				r.send(message{kind: "source_end", in: in, err: err})
				return
			}
			if !r.send(message{kind: "data", in: in, buffer: b}) {
				b.Release()
				return
			}
		}
	}
}
func (r *Relay) acceptSources(l *uri.Listener) {
	for {
		in, err := l.AcceptInput(r.stopCtx)
		if err != nil {
			if r.stopCtx.Err() == nil {
				e := l.Wait(context.Background())
				if e == nil {
					e = err
				}
				r.send(message{kind: "source_listener_error", err: e})
			}
			return
		}
		reply := make(chan bool, 1)
		if !r.send(message{kind: "source_ready", in: in, reply: reply}) {
			in.Cancel()
			in.Wait(context.Background())
			return
		}
		if !<-reply {
			in.Cancel()
			in.Wait(context.Background())
		}
	}
}
func (r *Relay) acceptDestinations(e Endpoint, l *uri.Listener) {
	for {
		out, err := l.AcceptOutput(r.stopCtx)
		if err != nil {
			if r.stopCtx.Err() == nil {
				cause := l.Wait(context.Background())
				if cause == nil {
					cause = err
				}
				r.send(message{kind: "destination_listener_error", e: e, err: cause})
			}
			return
		}
		reply := make(chan bool, 1)
		if !r.send(message{kind: "destination_ready", e: e, out: out, reply: reply}) {
			out.Cancel()
			out.Wait(context.Background())
			return
		}
		if !<-reply {
			out.Cancel()
			out.Wait(context.Background())
		}
	}
}
func (r *Relay) prepareFIFO(e Endpoint) {
	ctx, cancel := context.WithCancel(r.ctx)
	r.pending[e.ID] = cancel
	r.spawn(func() {
		out, err := uri.OpenOutput(ctx, e.URI, transportOptions(r.config, e))
		reply := make(chan bool, 1)
		if !r.send(message{kind: "destination_ready", e: e, out: out, err: err, cancel: cancel, reply: reply}) || !<-reply {
			if out != nil {
				out.Cancel()
				out.Wait(context.Background())
			}
			cancel()
		}
	})
}
func (r *Relay) install(e Endpoint, out *uri.Output, cancel context.CancelFunc, accepted bool) {
	id := e.ID
	if accepted {
		r.nextID++
		id = fmt.Sprintf("%s/%d", id, r.nextID)
	}
	d := newDestination(r, id, e, out, cancel)
	r.destinations[id] = d
	r.all[id] = d
	r.counters.active.Add(1)
	address := ""
	if out.Addr() != nil {
		address = out.Addr().String()
	}
	r.emit(Event{Kind: "destination_connected", ID: id, URI: e.URI, Address: address})
}
func (r *Relay) lose(d *destination, err error) {
	if _, ok := r.destinations[d.id]; !ok {
		return
	}
	delete(r.destinations, d.id)
	r.counters.active.Add(-1)
	r.failed = true
	r.emit(Event{Kind: "destination_failed", ID: d.id, URI: d.endpoint.URI, Error: err})
	d.abort()
}
func (r *Relay) run() {
	var joined <-chan struct{}
	var shutdown <-chan time.Time
	var shutdownTimer *time.Timer
	timeoutReported := false
	ctxDone := r.ctx.Done()
	closeRequest := r.closeRequest
	stop := func(err error, abort bool) {
		if r.stopping {
			return
		}
		r.stopping = true
		r.result = err
		r.stopCancel()
		for _, l := range r.listeners {
			l.Close()
		}
		for _, cancel := range r.pending {
			cancel()
		}
		if r.activeInput != nil {
			r.activeInput.Close()
		}
		if abort {
			r.discardAssembly()
		} else {
			r.flush()
		}
		for _, d := range r.destinations {
			if abort {
				d.abort()
			} else {
				d.finish()
			}
		}
		ch := make(chan struct{})
		joined = ch
		go func() { r.wg.Wait(); close(ch) }()
		shutdownTimer = time.NewTimer(r.config.ShutdownTimeout)
		shutdown = shutdownTimer.C
	}
	for {
		select {
		case <-ctxDone:
			cause := context.Cause(r.ctx)
			if errors.Is(cause, ErrShutdownTimeout) && !timeoutReported {
				r.emit(Event{Kind: "shutdown_timeout", Error: ErrShutdownTimeout})
				timeoutReported = true
			}
			ctxDone = nil
			if !r.stopping {
				stop(cause, true)
			} else {
				if !errors.Is(r.result, cause) {
					r.result = errors.Join(r.result, cause)
				}
				for _, d := range r.all {
					d.abort()
				}
			}
		case <-closeRequest:
			closeRequest = nil
			stop(nil, false)
		case <-shutdown:
			shutdown = nil
			if !errors.Is(r.result, ErrShutdownTimeout) {
				r.result = errors.Join(r.result, ErrShutdownTimeout)
			}
			if !timeoutReported {
				r.emit(Event{Kind: "shutdown_timeout", Error: ErrShutdownTimeout})
				timeoutReported = true
			}
			r.cancel(ErrShutdownTimeout)
			for _, d := range r.all {
				d.abort()
			}
		case <-r.tick:
			if !r.stopping {
				r.flush()
			}
		case m := <-r.messages:
			switch m.kind {
			case "data":
				if !r.stopping {
					r.counters.sourceBytes.Add(uint64(len(m.buffer.Data)))
					var err error
					if r.sourceDatagram {
						err = r.datagram(m.buffer.Data)
					} else {
						err = r.stream(m.buffer.Data)
					}
					m.buffer.Release()
					if err != nil {
						stop(err, true)
					}
				} else {
					m.buffer.Release()
				}
			case "source_ready":
				accepted := !r.stopping && r.activeInput == nil
				if accepted {
					r.activeInput = m.in
					r.emit(Event{Kind: "source_connected", ID: r.config.Source.ID, URI: r.config.Source.URI})
					r.spawn(func() { r.readInput(m.in) })
				} else if !r.stopping {
					r.emit(Event{Kind: "source_rejected", ID: r.config.Source.ID, Error: ErrLimit})
				}
				m.reply <- accepted
			case "source_end":
				r.activeInput = nil
				if r.stopping {
					break
				}
				r.emit(Event{Kind: "source_disconnected", ID: r.config.Source.ID, URI: r.config.Source.URI, Error: m.err})
				if r.config.Source.Listen && endpointScheme(r.config.Source) != "udp" {
					break
				}
				if endpointScheme(r.config.Source) == "fifo" && m.err == nil {
					in, err := uri.OpenInput(r.ctx, r.config.Source.URI, transportOptions(r.config, r.config.Source))
					if err != nil {
						stop(err, true)
					} else {
						r.activeInput = in
						r.spawn(func() { r.readInput(in) })
					}
				} else {
					stop(m.err, false)
				}
			case "destination_ready":
				delete(r.pending, m.e.ID)
				if m.err != nil {
					if !r.stopping {
						r.failed = true
						r.emit(Event{Kind: "destination_failed", ID: m.e.ID, URI: m.e.URI, Error: m.err})
					}
					m.reply <- false
					break
				}
				if r.stopping || len(r.all) >= r.config.MaxDestinations {
					if !r.stopping {
						r.emit(Event{Kind: "destination_rejected", ID: m.e.ID, URI: m.e.URI, Error: ErrLimit})
					}
					m.reply <- false
				} else {
					r.install(m.e, m.out, m.cancel, m.e.Listen)
					id := m.e.ID
					if m.e.Listen {
						id = fmt.Sprintf("%s/%d", m.e.ID, r.nextID)
					}
					r.startDestination(r.destinations[id])
					m.reply <- true
				}
			case "destination_error":
				if d := r.all[m.id]; d != nil && r.ctx.Err() == nil {
					r.lose(d, m.err)
				}
			case "destination_end":
				d := r.all[m.id]
				if d == nil {
					break
				}
				delete(r.all, m.id)
				if _, ok := r.destinations[m.id]; ok {
					if m.err != nil && r.ctx.Err() == nil {
						r.lose(d, m.err)
					} else {
						delete(r.destinations, m.id)
						r.counters.active.Add(-1)
					}
				}
			case "source_listener_error":
				if !r.stopping {
					r.emit(Event{Kind: "source_failed", ID: r.config.Source.ID, Error: m.err})
					stop(m.err, false)
				}
			case "destination_listener_error":
				if !r.stopping {
					r.failed = true
					r.emit(Event{Kind: "destination_failed", ID: m.e.ID, URI: m.e.URI, Error: m.err})
				}
			}
		case <-joined:
			close(r.stopCloseWatch)
			<-r.closeWatchDone
			if cause := context.Cause(r.ctx); cause != nil {
				if !errors.Is(r.result, cause) {
					r.result = errors.Join(r.result, cause)
				}
				if errors.Is(cause, ErrShutdownTimeout) && !timeoutReported {
					r.emit(Event{Kind: "shutdown_timeout", Error: ErrShutdownTimeout})
					timeoutReported = true
				}
			}
			for _, l := range r.listeners {
				if err := l.Wait(context.Background()); err != nil {
					r.result = errors.Join(r.result, err)
				}
			}
			if shutdownTimer != nil {
				shutdownTimer.Stop()
			}
			if r.timer != nil {
				r.timer.Stop()
			}
			if r.failed {
				r.result = errors.Join(r.result, ErrDestinationFailed)
			}
			r.counters.active.Store(0)
			r.destinations = nil
			r.all = nil
			r.pending = nil
			r.cancel(nil)
			close(r.events)
			close(r.done)
			return
		}
	}
}

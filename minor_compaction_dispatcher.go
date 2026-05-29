//go:build darwin || linux

package minweight_store

import (
	"errors"
	"sync"
)

type minorCompactionDispatcher struct {
	mu       sync.Mutex
	cond     *sync.Cond
	pending  bool
	stopping bool
	wg       sync.WaitGroup
}

func newMinorCompactionDispatcher() *minorCompactionDispatcher {
	d := &minorCompactionDispatcher{}
	d.cond = sync.NewCond(&d.mu)
	return d
}

func (s *Store) startMinorCompactionDispatcher() {
	if s.records == nil || s.manifest == nil || s.minorCompactionThreadNum <= 0 {
		return
	}

	d := newMinorCompactionDispatcher()
	d.wg.Add(1)
	s.minorCompaction = d

	go s.runMinorCompactionDispatcher(d)
}

func (s *Store) stopMinorCompactionDispatcher() {
	if s.minorCompaction == nil {
		return
	}
	s.minorCompaction.stop()
}

func (d *minorCompactionDispatcher) stop() {
	d.mu.Lock()
	d.stopping = true
	d.cond.Broadcast()
	d.mu.Unlock()
	d.wg.Wait()
}

func (s *Store) notifyMinorCompaction() {
	if s.minorCompaction == nil {
		return
	}
	s.minorCompaction.notify()
}

func (d *minorCompactionDispatcher) notify() {
	d.mu.Lock()
	if !d.stopping {
		d.pending = true
		d.cond.Signal()
	}
	d.mu.Unlock()
}

func (s *Store) runMinorCompactionDispatcher(d *minorCompactionDispatcher) {
	defer d.wg.Done()

	for {
		d.mu.Lock()
		for !d.pending && !d.stopping {
			d.cond.Wait()
		}
		if d.stopping {
			d.mu.Unlock()
			return
		}
		d.pending = false
		d.mu.Unlock()

		err := s.minorCompact()
		if err != nil {
			if !errors.Is(err, ErrClosed) {
				_ = s.mayMarkFatal(err)
			}
			return
		}
	}
}

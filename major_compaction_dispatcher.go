//go:build darwin || linux

package minweight_store

import (
	"errors"
	"sync"
)

type majorCompactionDispatcher struct {
	mu       sync.Mutex
	cond     *sync.Cond
	pending  bool
	stopping bool
	wg       sync.WaitGroup
}

func newMajorCompactionDispatcher() *majorCompactionDispatcher {
	d := &majorCompactionDispatcher{}
	d.cond = sync.NewCond(&d.mu)
	return d
}

func (s *Store) startMajorCompactionDispatcher() {
	if s.records == nil || s.manifest == nil || s.majorCompactionThreadNum <= 0 {
		return
	}

	d := newMajorCompactionDispatcher()
	d.wg.Add(1)
	s.majorCompaction = d

	go s.runMajorCompactionDispatcher(d)
}

func (s *Store) stopMajorCompactionDispatcher() {
	if s.majorCompaction == nil {
		return
	}
	s.majorCompaction.stop()
}

func (d *majorCompactionDispatcher) stop() {
	d.mu.Lock()
	d.stopping = true
	d.cond.Broadcast()
	d.mu.Unlock()
	d.wg.Wait()
}

func (s *Store) notifyMajorCompaction() {
	if s.majorCompaction == nil {
		return
	}
	s.majorCompaction.notify()
}

func (d *majorCompactionDispatcher) notify() {
	d.mu.Lock()
	if !d.stopping {
		d.pending = true
		d.cond.Signal()
	}
	d.mu.Unlock()
}

func (s *Store) runMajorCompactionDispatcher(d *majorCompactionDispatcher) {
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

		for {
			err := s.MajorCompact()
			if err != nil {
				if !errors.Is(err, ErrClosed) {
					_ = s.mayMarkFatal(err)
				}
				return
			}
			fileNos, err := s.majorCompactionSSTFileNos()
			if err != nil {
				if !errors.Is(err, ErrClosed) {
					_ = s.mayMarkFatal(err)
				}
				return
			}
			if len(fileNos) == 0 {
				break
			}
		}
	}
}

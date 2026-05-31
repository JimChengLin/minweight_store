//go:build darwin || linux

package minweight_store

import (
	"errors"
	"sync"
	"time"
)

type compactionDispatcher struct {
	mu       sync.Mutex
	cond     *sync.Cond
	pending  bool
	stopping bool
	wg       sync.WaitGroup
}

func newCompactionDispatcher() *compactionDispatcher {
	d := &compactionDispatcher{}
	d.cond = sync.NewCond(&d.mu)
	return d
}

func (d *compactionDispatcher) stop() {
	d.mu.Lock()
	d.stopping = true
	d.cond.Broadcast()
	d.mu.Unlock()
	d.wg.Wait()
}

func (d *compactionDispatcher) notify() {
	d.mu.Lock()
	if !d.stopping {
		d.pending = true
		d.cond.Signal()
	}
	d.mu.Unlock()
}

func (s *Store) runCompactionDispatcher(d *compactionDispatcher, name string, compact func() error) {
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

		start := time.Now()
		err := compact()
		if err != nil {
			logError(s.logger, "compaction_dispatcher_error", err,
				"name", name,
				"duration", time.Since(start),
			)
			if !errors.Is(err, ErrClosed) {
				_ = s.mayMarkFatal(err)
			}
			return
		}
	}
}

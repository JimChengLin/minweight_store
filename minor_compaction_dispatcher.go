//go:build darwin || linux

package minweight_store

func (s *Store) startMinorCompactionDispatcher() {
	if s.records == nil || s.manifest == nil || s.minorCompactionThreadNum <= 0 {
		return
	}

	d := newCompactionDispatcher()
	d.wg.Add(1)
	s.minorCompaction = d

	go s.runCompactionDispatcher(d, "minor_compaction", s.minorCompact)
}

func (s *Store) stopMinorCompactionDispatcher() {
	if s.minorCompaction == nil {
		return
	}
	s.minorCompaction.stop()
}

func (s *Store) notifyMinorCompaction() {
	if s.minorCompaction == nil {
		return
	}
	s.minorCompaction.notify()
}

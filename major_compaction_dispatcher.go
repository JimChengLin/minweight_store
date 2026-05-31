//go:build darwin || linux

package minweight_store

func (s *Store) startMajorCompactionDispatcher() {
	if s.records == nil || s.manifest == nil || s.majorCompactionThreadNum <= 0 {
		return
	}

	d := newCompactionDispatcher()
	d.wg.Add(1)
	s.majorCompaction = d

	go s.runCompactionDispatcher(d, "major_compaction", s.MajorCompact)
}

func (s *Store) stopMajorCompactionDispatcher() {
	if s.majorCompaction == nil {
		return
	}
	s.majorCompaction.stop()
}

func (s *Store) notifyMajorCompaction() {
	if s.majorCompaction == nil {
		return
	}
	s.majorCompaction.notify()
}

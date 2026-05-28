package minweight_store

import "github.com/JimChengLin/minpatricia"

type heapNodeStore struct {
	pages []*minpatricia.NodePage
	free  []uint64
	live  int
}

func newHeapNodeStore() *heapNodeStore {
	return &heapNodeStore{
		pages: []*minpatricia.NodePage{new(minpatricia.NodePage)},
		live:  1,
	}
}

func (s *heapNodeStore) Root() uint64 {
	return 0
}

func (s *heapNodeStore) Get(id uint64) (*minpatricia.NodePage, error) {
	if id >= uint64(len(s.pages)) || s.pages[id] == nil {
		return nil, minpatricia.ErrCorruptLayout
	}
	return s.pages[id], nil
}

func (s *heapNodeStore) Alloc() (uint64, *minpatricia.NodePage, error) {
	if len(s.free) != 0 {
		last := len(s.free) - 1
		id := s.free[last]
		s.free[last] = 0
		s.free = s.free[:last]
		page := new(minpatricia.NodePage)
		s.pages[id] = page
		s.live++
		return id, page, nil
	}

	id := uint64(len(s.pages))
	if id&minpatriciaHandleTag != 0 {
		return 0, nil, minpatricia.ErrPositionTag
	}
	page := new(minpatricia.NodePage)
	s.pages = append(s.pages, page)
	s.live++
	return id, page, nil
}

func (s *heapNodeStore) Free(id uint64) error {
	if id == s.Root() || id >= uint64(len(s.pages)) || s.pages[id] == nil {
		return minpatricia.ErrCorruptLayout
	}
	s.pages[id] = nil
	s.free = append(s.free, id)
	s.live--
	return nil
}

func (s *heapNodeStore) LiveNodes() int {
	return s.live
}

func (s *heapNodeStore) Sync() error {
	return nil
}

func (s *heapNodeStore) Close() error {
	return nil
}

func (s *heapNodeStore) closeAfterSync() error {
	return s.Close()
}

package graph

const MAX = int64(^uint64(0) >> 1)

type UIDPool struct {
	maxID      int64
	used, free Int64Set
}

func NewUIDPool() UIDPool {
	return UIDPool{
		maxID: -1,
		used:  make(Int64Set),
		free:  make(Int64Set),
	}
}
func (s *UIDPool) NewID() int64 {
	for id := range s.free {
		return id
	}
	if s.maxID != MAX {
		return s.maxID + 1
	}
	for id := int64(0); id <= s.maxID+1; id++ {
		if !s.used.Has(id) {
			return id
		}
	}
	panic("unreachable")
}
func (s *UIDPool) Use(id int64) {
	s.used.Add(id)
	s.free.Remove(id)
	if id > s.maxID {
		s.maxID = id
	}
}
func (s *UIDPool) Release(id int64) {
	s.free.Add(id)
	s.used.Remove(id)
}

package aap

type ByID []Node

func (ns ByID) Len() int { return len(ns)}
func (ns ByID) Less(i, j int) bool {
	return ns[i].ID() < ns[j].ID()
}
func (ns ByID) Swap(i, j int) {
	ns[i], ns[j] = ns[j], ns[i]
}

type BySliceValues [][]int64

func (ns BySliceValues) Len() int { return len(ns) }
func (ns BySliceValues) Less(i, j int) bool {
	a, b := ns[i], ns[j]
	l := len(a)
	if len(b) < l {
		l = len(b)
	}
	for k, v := range a[:l] {
		if v < b[k] {
			return true
		}
		if b[k] < v {
			return false
		}
	}
	return len(a) < len(b)
}
func (ns BySliceValues) Swap(i, j int) {
	ns[i], ns[j] = ns[j], ns[i]
}

type BySliceIDs [][]Node
func (ns BySliceIDs) Len() int { return len(ns) }
func (ns BySliceIDs) Less(i, j int) bool {
	a, b := ns[i], ns[j]
	l := len(a)
	if len(b) < l {
		l = len(b)
	}
	for k, v := range a[:l] {
		if v.ID() < b[k].ID() {
			return true
		}
		if b[k].ID() < v.ID() {
			return false
		}
	}
	return len(a) < len(b)
}
func (ns BySliceIDs) Swap(i, j int) {
	ns[i], ns[j] = ns[j], ns[i]
}

type Int64Slice []int64
func (s Int64Slice) Len() int           { return len(s) }
func (s Int64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func Reverse(nodes []Node) {
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
}
package aap

import (
	"unsafe"
)

type IntSet map[int]struct{}

func (s IntSet) Add(ele int) {
	s[ele] = struct{}{}
}
func (s IntSet) Has(ele int) bool {
	_, ok := s[ele]
	return ok
}
func (s IntSet) Remove(ele int) {
	delete(s, ele)
}
func (s IntSet) Count() int {
	return len(s)
}
func IntsEqual(a, b IntSet) bool {
	if isSameInts(a, b) {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for ele := range a {
		if _, ok := b[ele]; !ok {
			return false
		}
	}
	return true
}
func isSameInts(a, b IntSet) bool {
	return *(*uintptr)(unsafe.Pointer(&a)) == *(*uintptr)(unsafe.Pointer(&b))
}

type Int64Set map[int64]struct{}

func (s Int64Set) Add(ele int64) {
	s[ele] = struct{}{}
}
func (s Int64Set) Has(ele int64) bool {
	_, ok := s[ele]
	return ok
}
func (s Int64Set) Remove(ele int64) {
	delete(s, ele)
}
func (s Int64Set) Count() int {
	return len(s)
}
func Int64sEqual(a, b Int64Set) bool {
	if isSameInt64s(a, b) {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for ele := range a {
		if _, ok := b[ele]; !ok {
			return false
		}
	}
	return true
}
func isSameInt64s(a, b Int64Set) bool {
	return *(*uintptr)(unsafe.Pointer(&a)) == *(*uintptr)(unsafe.Pointer(&b))
}

type NodeSet map[int64]Node

func NewNodes() NodeSet {
	return make(NodeSet)
}
func newNodesSize(size int) NodeSet {
	return make(NodeSet, size)
}
func (ns NodeSet) Add(n Node) {
	ns[n.ID()] = n
}
func (ns NodeSet) Remove(n Node) {
	delete(ns, n.ID())
}
func (ns NodeSet) Count() int {
	return len(ns)
}
func (ns NodeSet) Has(n Node) bool {
	_, ok := ns[n.ID()]
	return ok
}
func CloneNodes(src NodeSet) NodeSet {
	dst := make(NodeSet, len(src))
	for idx, v := range src {
		dst[idx] = v
	}
	return dst
}
func UnionOfVertices(a, b NodeSet) NodeSet {
	if isSameNodes(a, b) {
		return CloneNodes(a)
	}
	dst := make(NodeSet)
	for idx, v := range a {
		dst[idx] = v
	}
	for idx, v := range b {
		dst[idx] = v
	}
	return dst
}
func IntersectionOnVertices(a, b NodeSet) NodeSet {
	if isSameNodes(a, b) {
		return CloneNodes(a)
	}
	dst := make(NodeSet)
	if len(a) > len(b) {
		a, b = b, a
	}
	for idx, v := range a {
		if _, ok := b[idx]; ok {
			dst[idx] = v
		}
	}
	return dst
}
func isSameNodes(a, b NodeSet) bool {
	return *(*uintptr)(unsafe.Pointer(&a)) == *(*uintptr)(unsafe.Pointer(&b))
}

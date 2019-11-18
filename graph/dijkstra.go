package graph

import "container/heap"

func DijkstraFrom(u Node, g WeightedDirectedGraph) *ShortestPath {
	shortestPath := NewShortestFrom(u, g.Nodes())
	Q := priorityQueue{{node: u, dist: 0}}
	for Q.Len() != 0 {
		mid := heap.Pop(&Q).(DistanceNode)
		k := shortestPath.index[mid.node.ID()]
		if mid.dist > shortestPath.dist[k] {
			continue
		}
		mnid := mid.node.ID()
		for _, n := range g.From(mnid) {
			nid := n.ID()
			j, ok := shortestPath.index[nid]
			if !ok {
				j = shortestPath.Add(n)
			}
			w, ok := g.Weight(mnid, nid)
			if !ok {
				panic("dijkstraFrom: unexpected invalid weight")
			}
			if w < 0 {
				panic("dijkstraFrom: negative edge weight")
			}
			joint := shortestPath.dist[k] + w
			if joint < shortestPath.dist[j] {
				heap.Push(&Q, DistanceNode{
					node: n,
					dist: joint,
				})
				shortestPath.Set(j, joint, k)
			}
		}
	}
	return shortestPath
}

type DistanceNode struct {
	node Node
	dist float64
}

func NewDistanceNode(node Node, dist float64) DistanceNode {
	return DistanceNode{
		node: node,
		dist: dist,
	}
}

type priorityQueue []DistanceNode

func (q priorityQueue) Len() int { return len(q) }
func (q priorityQueue) Less(i, j int) bool {return q[i].dist < q[j].dist }
func (q priorityQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *priorityQueue) Push(n interface{}) { *q = append(*q, n.(DistanceNode))}
func (q *priorityQueue) Pop() interface{} {
	t := *q
	var n interface{}
	n, *q = t[len(t)-1], t[:len(t) - 1]
	return n
}
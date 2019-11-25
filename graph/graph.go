package graph

import (
	"container/heap"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

type Node struct {
	id int64
}

func NewNode(id int64) Node {
	return Node{
		id: id,
	}
}
func (n Node) ID() int64 {
	return n.id
}

type WeightedEdge struct {
	F, T Node
	W    float64
}

func (e WeightedEdge) From() Node {
	return e.F
}
func (e WeightedEdge) To() Node {
	return e.T
}
func (e WeightedEdge) Weight() float64 {
	return e.W
}

type WeightedDirectedGraph struct {
	nodes   map[int64]Node
	from    map[int64]map[int64]WeightedEdge
	to      map[int64]map[int64]WeightedEdge
	self    float64
	absent  float64
	nodeIDs UIDPool
}

func NewWeightedDirectedGraph(self, absent float64) *WeightedDirectedGraph {
	return &WeightedDirectedGraph{
		nodes:   make(map[int64]Node),
		from:    make(map[int64]map[int64]WeightedEdge),
		to:      make(map[int64]map[int64]WeightedEdge),
		self:    self,
		absent:  absent,
		nodeIDs: NewUIDPool(),
	}
}
func NewWeightedDirectedGraphFromCSV(address string, self, absent float64) (*WeightedDirectedGraph, map[int64]Uint64Set) {
	g := NewWeightedDirectedGraph(self, absent)

	nodes, err := os.Open("data/nodes/nodes.txt")
	if err != nil {
		panic(err)
	}
	defer nodes.Close()

	possessionTable := make(map[int64]Uint64Set)

	reader := csv.NewReader(nodes)
	reader.Read() // skip header
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		nodeID, _ := strconv.ParseInt(record[0], 10, 64)
		workerIDs := strings.Split(record[1], ":")
		for _, _id := range workerIDs {
			id, _ := strconv.ParseInt(_id, 10, 64)
			possessionTable[nodeID].Add(uint64(id))
		}
	}

	edges, err := os.Open("data/edges/" + address + ".txt")
	if err != nil {
		panic(err)
	}
	defer edges.Close()

	reader = csv.NewReader(edges)
	reader.Read() // skip header
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		fid, _ := strconv.ParseInt(record[0], 10, 64)
		tid, _ := strconv.ParseInt(record[1], 10, 64)
		weight, _ := strconv.ParseFloat(record[2], 64)

		weightedEdge := WeightedEdge{
			F: NewNode(fid),
			T: NewNode(tid),
			W: weight,
		}

		g.SetWeightedEdge(weightedEdge)
	}
	return g, possessionTable
}
func (g *WeightedDirectedGraph) Node(id int64) Node {
	return g.nodes[id]
}
func (g *WeightedDirectedGraph) Nodes() []Node {
	if len(g.nodes) == 0 {
		return make([]Node, 0)
	}
	nodes := make([]Node, len(g.nodes))
	i := 0
	for _, n := range g.nodes {
		nodes[i] = n
		i++
	}
	return nodes
}
func (g *WeightedDirectedGraph) From(id int64) []Node {
	if _, ok := g.from[id]; !ok {
		return make([]Node, 0)
	}
	from := make([]Node, len(g.from[id]))
	i := 0
	for nid := range g.from[id] {
		from[i] = g.nodes[nid]
		i++
	}
	if len(from) == 0 {
		return make([]Node, 0)
	}
	return from
}
func (g *WeightedDirectedGraph) To(id int64) []Node {
	if _, ok := g.to[id]; !ok {
		return make([]Node, 0)
	}
	to := make([]Node, len(g.to[id]))
	i := 0
	for uid := range g.to[id] {
		to[i] = g.nodes[uid]
		i++
	}
	if len(to) == 0 {
		return make([]Node, 0)
	}
	return to
}
func (g *WeightedDirectedGraph) AddNode(n Node) {
	if _, exists := g.nodes[n.ID()]; exists {
		panic(fmt.Sprintf("WeightedDirectedGraph: node ID collision: %d", n.ID()))
	}
	g.nodes[n.ID()] = n
	g.nodeIDs.Use(n.ID())
}
func (g *WeightedDirectedGraph) RemoveNode(id int64) {
	if _, ok := g.nodes[id]; !ok {
		return
	}
	delete(g.nodes, id)
	for from := range g.from[id] {
		delete(g.to[from], id)
	}
	delete(g.from, id)
	for to := range g.to[id] {
		delete(g.from[to], id)
	}
	delete(g.to, id)
	g.nodeIDs.Release(id)
}
func (g *WeightedDirectedGraph) WeightedEdge(uid, vid int64) WeightedEdge {
	edge, ok := g.from[uid][vid]
	if !ok {
		panic(fmt.Sprintf("WeightedDirectedGraph: no such edge <uid=%d -- vid=%d>", uid, vid))
	}
	return edge
}
func (g *WeightedDirectedGraph) WeightedEdges() []WeightedEdge {
	var edges []WeightedEdge
	for _, u := range g.nodes {
		for _, e := range g.from[u.ID()] {
			edges = append(edges, e)
		}
	}
	if len(edges) == 0 {
		return make([]WeightedEdge, 0)
	}
	return edges
}
func (g *WeightedDirectedGraph) HasWeightedEdgeBetween(xid, yid int64) bool {
	if _, ok := g.from[xid][yid]; ok {
		return true
	}
	_, ok := g.from[yid][xid]
	return ok
}
func (g *WeightedDirectedGraph) HasWeightedEdgeFromTo(uid, vid int64) bool {
	if _, ok := g.from[uid][vid]; !ok {
		return false
	}
	return true
}
func (g *WeightedDirectedGraph) Weight(xid, yid int64) (w float64, ok bool) {
	if xid == yid {
		return g.self, true
	}
	if to, ok := g.from[xid]; ok {
		if e, ok := to[yid]; ok {
			return e.W, true
		}
	}
	return g.absent, false
}
func (g *WeightedDirectedGraph) SetWeightedEdge(we WeightedEdge) {
	var (
		from = we.From()
		fid  = from.ID()
		to   = we.To()
		tid  = to.ID()
	)
	if fid == tid {
		panic("WeightedDirectedGraph: adding self weighted edge")
	}
	if _, ok := g.nodes[fid]; !ok {
		g.AddNode(from)
	} else {
		g.nodes[fid] = from
	}
	if _, ok := g.nodes[tid]; !ok {
		g.AddNode(to)
	} else {
		g.nodes[tid] = to
	}

	if fm, ok := g.from[fid]; ok {
		fm[tid] = we
	} else {
		g.from[fid] = map[int64]WeightedEdge{tid: we}
	}
	if tm, ok := g.to[tid]; ok {
		tm[fid] = we
	} else {
		g.to[tid] = map[int64]WeightedEdge{fid: we}
	}
}
func (g *WeightedDirectedGraph) RemoveWeightedEdge(fid, tid int64) {
	if _, ok := g.nodes[fid]; !ok {
		return
	}
	if _, ok := g.nodes[tid]; !ok {
		return
	}
	delete(g.from[fid], tid)
	delete(g.to[tid], fid)
}

type ShortestPath struct {
	from             Node
	nodes            []Node
	index            map[int64]int
	invIndex         map[int]int64
	dist             []float64
	next             []int
	hasNegativeCycle bool
	changed          Int64Set
}

func NewShortestFrom(u Node, nodes []Node) *ShortestPath {
	index := make(map[int64]int, len(nodes))
	invIndex := make(map[int]int64, len(nodes))
	uid := u.ID()
	for i, n := range nodes {
		index[n.ID()] = i
		if n.ID() == uid {
			u = n
		}
	}
	for k, v := range index {
		invIndex[v] = k
	}
	p := ShortestPath{
		from:             u,
		nodes:            nodes,
		index:            index,
		invIndex:         invIndex,
		dist:             make([]float64, len(nodes)),
		next:             make([]int, len(nodes)),
		hasNegativeCycle: false,
		changed:          map[int64]struct{}{},
	}
	for i := range nodes {
		p.dist[i] = math.Inf(1)
		p.next[i] = -1
	}
	p.dist[index[uid]] = 0
	return &p
}
func (s ShortestPath) From() Node {
	return s.from
}
func (s ShortestPath) To(vid int64) (path []Node, weight float64) {
	to, ok := s.index[vid]
	if !ok || math.IsInf(s.dist[to], 1) {
		return nil, math.Inf(1)
	}
	from := s.index[s.from.ID()]
	path = []Node{s.nodes[to]}
	weight = math.Inf(1)
	if s.hasNegativeCycle {
		seen := make(IntSet)
		seen.Add(from)
		for to != from {
			if seen.Has(to) {
				weight = math.Inf(-1)
				break
			}
			seen.Add(to)
			path = append(path, s.nodes[s.next[to]])
			to = s.next[to]
		}
	} else {
		n := len(s.nodes)
		for to != from {
			path = append(path, s.nodes[s.next[to]])
			to = s.next[to]
			if n < 0 {
				panic("path: unexpected negative cycle")
			}
			n--
		}
	}
	Reverse(path)
	return path, math.Min(weight, s.dist[s.index[vid]])
}
func (s ShortestPath) Nodes() []Node {
	return s.nodes
}
func (s ShortestPath) Dist() []float64 {
	return s.dist
}
func (s ShortestPath) DistOf(id int64) float64 {
	return s.dist[s.index[id]]
}
func (s ShortestPath) Index() map[int64]int {
	return s.index
}
func (s ShortestPath) InvIndex() map[int]int64 {
	return s.invIndex
}
func (s ShortestPath) Add(u Node) int {
	uid := u.ID()
	if _, exists := s.index[uid]; exists {
		panic("shortestPath: adding existing node")
	}
	idx := len(s.nodes)
	s.index[uid] = idx
	s.nodes = append(s.nodes, u)
	s.dist = append(s.dist, math.Inf(1))
	s.next = append(s.next, -1)
	return idx
}
func (s ShortestPath) Set(to int, weight float64, mid int) {
	s.dist[to] = weight
	s.next[to] = mid
	s.changed[s.invIndex[to]] = struct{}{}
}
func (s ShortestPath) ChangedNodeIDs() []int64 {
	ns := make([]int64, len(s.changed))
	i := 0
	for k := range s.changed {
		ns[i] = k
	}
	return ns
}
func (s ShortestPath) ClearChanged() {
	s.changed.Clear()
}
func (s ShortestPath) Result() map[int64]float64 {
	result := make(map[int64]float64)
	for nid, idx := range s.index {
		result[nid] = s.dist[idx]
	}
	return result
}

func PEvalDijkstra(graph *WeightedDirectedGraph, path *ShortestPath) {
	Q := priorityQueue{NewDistanceNode(0, 0)}
	for Q.Len() != 0 {
		mid := heap.Pop(&Q).(DistanceNode)
		k := path.index[mid.node.ID()]
		if mid.dist > path.dist[k] {
			continue
		}
		mnid := mid.node.ID()
		for _, n := range graph.From(mnid) {
			vid := n.ID()
			j, ok := path.index[vid]
			if !ok {
				j = path.Add(n)
			}
			w, ok := graph.Weight(mnid, vid)
			if !ok {
				panic("dijkstra: unexpected invalid weight")
			}
			if w < 0 {
				panic("dijkstra: negative edge weight")
			}
			joint := path.dist[k] + w
			if joint < path.dist[j] {
				heap.Push(&Q, DistanceNode{node: n, dist: joint})
				path.Set(j, joint, k)
			}
		}
	}
}

func IncEvalDijkstra(updates []DistanceNode, graph *WeightedDirectedGraph, path *ShortestPath) {
	Q := priorityQueue{}
	for _, u := range updates {
		heap.Push(&Q, u)
	}
	for Q.Len() != 0 {
		mid := heap.Pop(&Q).(DistanceNode)
		k := path.index[mid.node.ID()]
		if mid.dist > path.dist[k] {
			continue
		}
		mnid := mid.node.ID()
		for _, n := range graph.From(mnid) {
			vid := n.ID()
			j, ok := path.index[vid]
			if !ok {
				j = path.Add(n)
			}
			w, ok := graph.Weight(mnid, vid)
			if !ok {
				panic("dijkstra: unexpected invalid weight")
			}
			if w < 0 {
				panic("dijkstra: negative edge weight")
			}
			joint := path.dist[k] + w
			if joint < path.dist[j] {
				heap.Push(&Q, DistanceNode{node: n, dist: joint})
				path.Set(j, joint, k)
			}
		}
	}
}

func isSame(a, b float64) bool {
	return a == b || (math.IsNaN(a) && math.IsNaN(b))
}

type DistanceNode struct {
	node Node
	dist float64
}

func NewDistanceNode(nid int64, dist float64) DistanceNode {
	return DistanceNode{
		node: NewNode(nid),
		dist: dist,
	}
}

type priorityQueue []DistanceNode

func (pq priorityQueue) Len() int { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool { return pq[i].dist < pq[j].dist }
func (pq priorityQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }
func (pq *priorityQueue) Push(n interface{}) {
	*pq = append(*pq, n.(DistanceNode))
}
func (pq *priorityQueue) Pop() interface{} {
	t := *pq
	var n interface{}
	n, *pq = t[len(t)-1], t[:len(t)-1]
	return n
}
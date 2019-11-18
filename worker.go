package aap

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/zakimal/aap/graph"
	"github.com/zakimal/aap/log"
	"github.com/zakimal/aap/transport"
	"math"
	"net"
	"sync"
	"sync/atomic"
)

type Worker struct {
	ID                    int64
	transport             transport.Transport
	listener              net.Listener
	host                  string
	port                  uint16
	peers                 map[int64]*Peer
	recvQueue             sync.Map // map[opcode]ReceiveHandle
	weightedDirectedGraph *graph.WeightedDirectedGraph
	shortest              *graph.ShortestPath
	kill                  chan chan struct{}
	killOnce              uint32
}


func NewWorker(id int64, host string, port uint16) (*Worker, error) {
	tcp := transport.NewTCP()
	listener, err := tcp.Listen(host, port)
	if err != nil {
		return nil, errors.Errorf("failed to create listener for peers on port %d", port)
	}
	weightedDirectedGraph := graph.NewWeightedDirectedGraphFromCSV(id, 0.0, math.Inf(1))
	nodes := weightedDirectedGraph.Nodes()
	from := weightedDirectedGraph.Node(0)
	worker := Worker{
		ID:                    id,
		transport:             tcp,
		listener:              listener,
		host:                  host,
		port:                  port,
		peers:                 make(map[int64]*Peer),
		weightedDirectedGraph: weightedDirectedGraph,
		shortest:              graph.NewShortestFrom(from, nodes),
		kill:                  make(chan chan struct{}, 1),
		killOnce:              0,
	}
	return &worker, nil
}
func (w *Worker) Address() string {
	return fmt.Sprintf("%s:%d", w.host, w.port)
}
func (w *Worker) Host() string {
	return w.host
}
func (w *Worker) Port() uint16 {
	return w.port
}
func (w *Worker) Listen() {
	for {
		select {
		case signal := <-w.kill:
			close(signal)
			return
		default:
		}
		conn, err := w.listener.Accept()
		if err != nil {
			panic(err)
		}
		peer := NewPeer(w, conn)
		peer.init()
		w.SetPeer(peer)
		log.Info().Msgf("Accepted connection with peer from %s", peer.RemoteAddress())
	}
}
func (w *Worker) Dial(address string) (*Peer, error) {
	conn, err := w.transport.Dial(address)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to setupTestPeer %s", conn)
	}
	peer := NewPeer(w, conn)
	peer.init()
	w.SetPeer(peer)
	log.Info().Msgf("Connected with peer at %s", address)
	return peer, nil
}
func (w *Worker) Receive(op Opcode) <-chan Message {
	c, _ := w.recvQueue.LoadOrStore(op, ReceiveHandle{hub: make(chan Message), lock: make(chan struct{}, 1)})
	return c.(ReceiveHandle).hub
}
func (w *Worker) LockOnReceive(op Opcode) ReceiveHandle {
	c, _ := w.recvQueue.LoadOrStore(op, ReceiveHandle{hub: make(chan Message), lock: make(chan struct{}, 1)})
	recv := c.(ReceiveHandle)
	recv.lock <- struct{}{}
	return recv
}
func (w *Worker) RegisterPeer(peer *Peer) {

}
func (w *Worker) Peers() map[int64]*Peer {
	return w.peers
}
func (w *Worker) Peer(id int64) *Peer {
	return w.peers[id]
}
func (w *Worker) Broadcast(message Message) {
	for _, peer := range w.peers {
		peer.SendMessageAsync(message)
		log.Info().Msgf("send %+v to %+v", message, peer)
	}
}
func (w *Worker) SetPeer(peer *Peer) {
	w.peers[peer.ID] = peer
}
func (w *Worker) RemovePeer(id int64) {
	delete(w.peers, id)
}
func (w *Worker) Fence() {
	<-w.kill
}
func (w *Worker) Kill() {
	if !atomic.CompareAndSwapUint32(&w.killOnce, 0, 1) {
		return
	}
	signal := make(chan struct{})
	w.kill <- signal
	if err := w.listener.Close(); err != nil {
		panic(err)
	}
	<-signal
	close(w.kill)
}

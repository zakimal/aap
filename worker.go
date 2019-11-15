package aap

import (
	"github.com/pkg/errors"
	"github.com/zakimal/aap/log"
	"math"
	"fmt"
	"net"
	"sync/atomic"
)

type Worker struct {
	ID        string
	transport tcp
	listener  net.Listener
	host      string
	port      uint16
	Peers     []*Peer
	graph     *WeightedDirectedGraph
	shortest  Shortest
	kill      chan chan struct{}
	killOnce  uint32
}

func NewWorker(id string, host string, port uint16) (*Worker, error) {
	tcp := NewTCP()
	listener, err := tcp.Listen(host, port)
	if err != nil {
		return nil, errors.Errorf("failed to create listener for Peers on port %d", port)
	}
	graph := NewWeightedDirectedGraphFromCSV(id,0.0, math.Inf(1))
	nodes := graph.Nodes()
	from := graph.Node(0)
	worker := Worker{
		ID:        id,
		transport: tcp,
		listener:  listener,
		host:      host,
		port:      port,
		Peers:     make([]*Peer, 0),
		graph:     graph,
		shortest:  NewShortestFrom(from, nodes),
		kill:      make(chan chan struct{}, 1),
		killOnce:  0,
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
		case signal := <- w.kill:
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
		log.Info().Msgf("accepted connection with peer from %s", peer.RemoteAddress())
	}
}
func (w *Worker) Dial(address string) (*Peer, error) {
	conn, err := w.transport.Dial(address)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to setupTestPeer %s", conn)
	}
	peer := NewPeer(w, conn)
	peer.init()
	log.Info().Msgf("connected with peer at %s", peer.RemoteAddress())
	return peer, nil
}
func (w *Worker) Broadcast(message Message) {
	for _, peer := range w.Peers {
		log.Info().Msgf("sending %+v to %+v", message, peer)
		peer.SendMessageAsync(message)
	}
}
func (w *Worker) SetPeer(peer *Peer) {
	w.Peers = append(w.Peers, peer)
}
func (w *Worker) RemovePeer(peer *Peer) {
	peers := make([]*Peer, 0)
	for _, p := range w.Peers {
		if p.RemoteAddress() != peer.RemoteAddress() {
			peers = append(peers, p)
		}
	}
	w.Peers = peers
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
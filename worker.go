package aap

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"github.com/zakimal/aap/graph"
	"github.com/zakimal/aap/log"
	"github.com/zakimal/aap/payload"
	"github.com/zakimal/aap/transport"
	"math"
	"net"
	"sync"
	"sync/atomic"
)

type WSendHandle struct {
	to      *Peer
	payload []byte
	result  chan error
}

type WReceiveHandle struct {
	hub  chan Message
	lock chan struct{}
}

func (r *WReceiveHandle) Unlock() {
	<-r.lock
}

type Worker struct {
	ID                    uint64
	transport             transport.Transport
	listener              net.Listener
	host                  string
	port                  uint16
	peers                 map[uint64]*Peer
	sendQueue             chan WSendHandle
	recvQueue             sync.Map
	round                 uint64
	weightedDirectedGraph *graph.WeightedDirectedGraph
	shortestPath          *graph.ShortestPath
	possessionTable       map[int64]graph.Uint64Set // node ID => worker ID set
	kill                  chan chan struct{}
	killOnce              uint32
}

func NewWorker(id uint64, host string, port uint16) (*Worker, error) {
	tcp := transport.NewTCP()
	listener, err := tcp.Listen(host, port)
	if err != nil {
		return nil, errors.Errorf("failed to create listener for peers on port %d", port)
	}
	address := fmt.Sprintf("%s:%d", host, port)
	weightedDirectedGraph, possessionTable := graph.NewWeightedDirectedGraphFromCSV(address, 0.0, math.Inf(1))
	nodes := weightedDirectedGraph.Nodes()
	from := weightedDirectedGraph.Node(0)
	shortestPath := graph.NewShortestFrom(from, nodes)
	worker := Worker{
		ID:                    id,
		transport:             tcp,
		listener:              listener,
		host:                  host,
		port:                  port,
		peers:                 make(map[uint64]*Peer),
		sendQueue:             make(chan WSendHandle, 128),
		recvQueue:             nil,
		round:                 0,
		weightedDirectedGraph: weightedDirectedGraph,
		shortestPath:          shortestPath,
		possessionTable:       possessionTable,
		kill:                  make(chan chan struct{}, 1),
		killOnce:              0,
	}
	return &worker, nil
}

func (w *Worker) Init() {
	go w.messageSender()
}
func (w *Worker) messageSender() {
	for {
		var cmd WSendHandle
		select {
		case cmd = <-w.sendQueue:
			cmd.to.sendQueue <- PSendHandle{
				payload: cmd.payload,
				result:  cmd.result,
			}
		}
	}
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
			continue
		}
		peer := NewPeer(w, conn)
		peer.Init()
	}
}

func (w *Worker) WeightedDirectedGraph() *graph.WeightedDirectedGraph {
	return w.weightedDirectedGraph
}
func (w *Worker) ShortestPath() *graph.ShortestPath {
	return w.shortestPath
}
func (w *Worker) PossessionTable() map[int64]graph.Uint64Set {
	return w.possessionTable
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
func (w *Worker) EncodeMessage(message Message) ([]byte, error) {
	opcode, err := OpcodeFromMessage(message)
	if err != nil {
		return nil, errors.Wrap(err, "could not find opcode registered for messageReceiver")
	}
	var buf bytes.Buffer
	_, err = buf.Write(payload.NewWriter(nil).WriteByte(byte(opcode)).Bytes())
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize messageReceiver opcode")
	}
	_, err = buf.Write(message.Write())
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize and write messageReceiver contents")
	}
	return buf.Bytes(), nil
}

func (w *Worker) SendMessage(peer *Peer, message Message) error {
	payload, err := w.EncodeMessage(message)
	if err != nil {
		return errors.Wrap(err, "failed to serialize message contents to be sent to a peer")
	}
	cmd := WSendHandle{
		to:      peer,
		payload: payload,
		result:  make(chan error, 1),
	}
	select {
	case w.sendQueue <- cmd:
	}
	select {
	case err = <-cmd.result:
		return err
	}
}
func (w *Worker) SendMessageAsync(peer *Peer, message Message) <-chan error {
	result := make(chan error, 1)
	payload, err := w.EncodeMessage(message)
	if err != nil {
		result <- errors.Wrap(err, "failed to serialize message contents to be sent to a peer")
		return result
	}
	cmd := WSendHandle{
		to:      peer,
		payload: payload,
		result:  result,
	}
	select {
	case w.sendQueue <- cmd:
	}
	return result
}
func (w *Worker) Receive(op Opcode) <-chan Message {
	c, _ := w.recvQueue.LoadOrStore(op, WReceiveHandle{
		hub:  make(chan Message),
		lock: make(chan struct{}, 1),
	})
	return c.(WReceiveHandle).hub
}
func (w *Worker) Disconnect(peer *Peer) {
	if !atomic.CompareAndSwapUint32(&peer.killOnce, 0, 1) {
		return
	}
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		peer.kill <- &wg
	}
	if err := peer.conn.Close(); err != nil {
		log.Info().Msg(errors.Wrapf(err, "got errors closing peer connection").Error())
	}
	wg.Wait()
	close(peer.kill)
}
func (w *Worker) DisconnectAsync(peer *Peer) <-chan struct{} {
	signal := make(chan struct{})
	if !atomic.CompareAndSwapUint32(&peer.killOnce, 0, 1) {
		close(signal)
		return signal
	}
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		peer.kill <- &wg
	}
	if err := peer.conn.Close(); err != nil {
		log.Info().Msg(errors.Wrapf(err, "got errors closing peer connection").Error())
	}
	go func() {
		wg.Wait()
		close(peer.kill)
		close(signal)
	}()
	return signal
}

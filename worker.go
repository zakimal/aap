package aap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/zakimal/aap/graph"
	"github.com/zakimal/aap/log"
	"github.com/zakimal/aap/payload"
	"github.com/zakimal/aap/transport"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

type sendHandle struct {
	to *Peer
	payload []byte
	result chan error
}

type receiveHandle struct {
	hub chan Message
	lock chan struct{}
}

type Worker struct {
	id uint64
	transport transport.Transport
	listener net.Listener
	host string
	port uint16
	peers map[uint64]*Peer
	sendQueue chan sendHandle
	recvQueue sync.Map
	round uint64
	weightedDirectedGraph *graph.WeightedDirectedGraph
	shortestPath *graph.ShortestPath
	possessionTable map[int64]graph.Uint64Set // node ID => worker ID
	kill chan chan struct{}
	killOnce uint32
}

func NewWorker(id uint64, host string, port uint16) (*Worker, error) {
	tcp := transport.NewTCP()
	listener, err := tcp.Listen(host, port)
	if err != nil {
		return nil, errors.Errorf("failed to create listener for peers on port %d", port)
	}
	weightedDirectedGraph, possessionTable := graph.NewWeightedDirectedGraphFromCSV()
	shortestPath := graph.NewShortestFrom()
	worker := Worker{
		id:                    id,
		transport:             tcp,
		listener:              listener,
		host:                  host,
		port:                  port,
		peers:                 make(map[uint64]*Peer),
		sendQueue:             make(chan sendHandle, 128),
		recvQueue:             nil,
		round:                 0,
		weightedDirectedGraph: weightedDirectedGraph,
		shortestPath:          shortestPath,
		possessionTable:       possessionTable,
		kill:                  make(chan chan struct{}, 1),
		killOnce:              0,
	}
	worker.init() // go worker.messageSender()
	return &worker, nil
}

func (w *Worker) init() {
	go w.messageSender()
}

func (w *Worker) messageSender() {
	for {
		var cmd sendHandle
		select {
		case cmd = <- w.sendQueue:
		}
		to := cmd.to
		payload := cmd.payload
		size := len(payload)
		buf := make([]byte, binary.MaxVarintLen64)
		prepend := binary.PutUvarint(buf[:], uint64(size))
		buf = append(buf[:prepend], payload[:]...)
		copied, err := io.Copy(to.conn, bytes.NewReader(buf))
		if copied != int64(size+prepend) {
			if cmd.result != nil {
				cmd.result <- errors.Errorf(
					"only written %d bytes when expected to write %d bytes to peer\n",
					copied, size+prepend)
				close(cmd.result)
			}
			continue
		}
		if err != nil {
			if cmd.result != nil {
				cmd.result <- errors.Wrap(err, "failed to send message to peer")
				close(cmd.result)
			}
			continue
		}
		if cmd.result != nil {
			cmd.result <- nil
			close(cmd.result)
		}
	}
}

func (w *Worker) SendMessage(to *Peer, message Message) error {
	payload, err := w.encodeMessage(message)
	if err != nil {
		return errors.Wrap(err, "failed to serialize message contents to be sent to a peer")
	}
	cmd := sendHandle{
		to:      to,
		payload: payload,
		result:  make(chan error, 1),
	}
	select {
	case w.sendQueue <- cmd:
	}
	select {
	case err = <- cmd.result:
		return err
	}
}
func (w *Worker) SendMessageAsync(to *Peer, message Message) <- chan error {
	result := make(chan error, 1)
	payload, err := w.encodeMessage(message)
	if err != nil {
		result <- errors.Wrap(err, "failed to serialize message contents to be sent to a peer")
		return result
	}
	cmd := sendHandle{
		to:      to,
		payload: payload,
		result:  result,
	}
	select {
	case w.sendQueue <- cmd:
	}
	return result
}
func (w *Worker) encodeMessage(message Message) ([]byte, error) {
	opcode, err := OpcodeFromMessage(message)
	if err != nil {
		return nil, errors.Wrap(err, "could not find opcode registered for message")
	}
	var buf bytes.Buffer
	_, err = buf.Write(payload.NewWriter(nil).WriteByte(byte(opcode)).Bytes())
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize message opcode")
	}
	_, err = buf.Write(message.Write())
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize and write message contents")
	}
	return buf.Bytes(), nil
}

func (w *Worker) Receive(opcode Opcode) <- chan Message {
	c, _ := w.recvQueue.LoadOrStore(opcode, receiveHandle{
		hub:  make(chan Message),
		lock: make(chan struct{}, 1),
	})
	return c.(receiveHandle).hub
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
			continue
		}
		peer := NewPeer(w, conn)
		peer.init() // go peer.messageReceiver()
		// TODO: ここにいろいろ書けばpeerごとに実行される
	}
}
func (w *Worker) Dial(address string) (*Peer, error) {
	conn, err := w.transport.Dial(address)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to peer %s", conn)
	}
	peer := NewPeer(w, conn)
	peer.init() // go peer.messageReceiver()
	// TODO: ここにいろいろ書けばpeerごとに実行される
	return peer, nil
}

func (w *Worker) Disconnect(peer *Peer) {
	id := peer.id
	delete(w.peers, id)
	peer.Disconnect()
}
func (w *Worker) DisconnectAsync(peer *Peer) <- chan struct{} {
	id := peer.id
	delete(w.peers, id)
	return peer.DisconnectAsync()
}

func (w *Worker) ID() uint64 {
	return w.id
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
func (w *Worker) Peers() map[uint64]*Peer {
	return w.peers
}
func (w *Worker) WeightedDirectedGraph() *graph.WeightedDirectedGraph {
	return w.weightedDirectedGraph
}
func (w *Worker) ShortestPath() *graph.ShortestPath {
	return w.shortestPath
}
func (w *Worker) Round() uint64 {
	return w.round
}
func (w *Worker) PossessionTable() map[int64]graph.Uint64Set {
	return w.possessionTable
}
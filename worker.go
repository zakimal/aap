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
	"math"
	"net"
	"sync"
)

type sendHandle struct {
	to      *Peer
	payload []byte
	result  chan error
}

type receiveHandle struct {
	hub  chan Message
	lock chan struct{}
}

type Worker struct {
	id                    uint64
	transport             transport.Transport
	listener              net.Listener
	host                  string
	port                  uint16
	peers                 map[uint64]*Peer
	sendQueue             chan sendHandle
	recvQueue             sync.Map
	round                 uint64
	weightedDirectedGraph *graph.WeightedDirectedGraph
	shortestPath          *graph.ShortestPath
	possessionTable       map[int64]graph.Uint64Set // node ID => worker ID
	master                *Peer
	isInactive            bool
	inactiveMap map[uint64]bool
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
	shortestPath := graph.NewShortestFrom(weightedDirectedGraph.Node(0), weightedDirectedGraph.Nodes())
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
		isInactive:            false,
		inactiveMap: make(map[uint64]bool),
		kill:                  make(chan chan struct{}, 1),
		killOnce:              0,
	}
	worker.init() // go worker.messageSender()
	return &worker, nil
}

// opcodes
var (
	opcodeHello            Opcode
	opcodePEvalRequest     Opcode
	opcodePEvalResponse    Opcode
	opcodeIncEvalUpdate    Opcode
	opcodeNotifyInactive   Opcode
	opcodeTerminate        Opcode
	opcodeTerminateACK     Opcode
	opcodeAssembleRequest  Opcode
	opcodeAssembleResponse Opcode
	// TODO: opcode
)

// register messages and spawn message sender
func (w *Worker) init() {
	opcodeHello = RegisterMessage(NextAvailableOpcode(), (*MessageHello)(nil))
	opcodePEvalRequest = RegisterMessage(NextAvailableOpcode(), (*MessagePEvalRequest)(nil))
	opcodePEvalResponse = RegisterMessage(NextAvailableOpcode(), (*MessagePEvalResponse)(nil))
	opcodeIncEvalUpdate = RegisterMessage(NextAvailableOpcode(), (*MessageIncEvalUpdate)(nil))
	opcodeNotifyInactive = RegisterMessage(NextAvailableOpcode(), (*MessageNotifyInactive)(nil))
	opcodeTerminate = RegisterMessage(NextAvailableOpcode(), (*MessageTerminate)(nil))
	opcodeTerminateACK = RegisterMessage(NextAvailableOpcode(), (*MessageTerminateACK)(nil))
	opcodeAssembleRequest = RegisterMessage(NextAvailableOpcode(), (*MessageAssembleRequest)(nil))
	opcodeAssembleResponse = RegisterMessage(NextAvailableOpcode(), (*MessageAssembleResponse)(nil))
	go w.messageSender()

	go func() {
		for {
			select {
			case msg := <- w.Receive(opcodePEvalRequest):
				log.Info().Msgf("received PEval request from peer %d", msg.(MessagePEvalRequest).from)
				master := w.peers[msg.(MessagePEvalRequest).from]
				w.master = master
				log.Info().Msgf("set peer %d as master", master.id)
				if err := w.SendMessage(master, MessagePEvalResponse{debugText: fmt.Sprintf("OK from %d", w.id)}) ;err != nil {
					panic(err)
				}
				// PEvalでdistが更新された頂点集合はChangedNodeIDs()で取れる
				// 更新された頂点が自分以外のworker所有かつ自分の所有している頂点を始点とする辺の終点であるならば送信する
				// TODO: グラフを読み込んだ時点で，F.I/F.O/F.I'/F.O'を計算しておくべき
				graph.PEvalDijkstra(w.weightedDirectedGraph, w.shortestPath)
				for _, nid := range w.shortestPath.ChangedNodeIDs() {
					owners := w.possessionTable[nid]
					for owner := range owners {

					}
				}
				// TODO: IMPLEMENT PEval & send messages...
			case msg := <- w.Receive(opcodePEvalResponse):
				log.Info().Msgf("received PEval response from peer %d", msg.(MessagePEvalResponse).from)
			case msg := <- w.Receive(opcodeIncEvalUpdate):
				log.Info().Msgf("received PEval request from peer %d: <from=%d, round=%d, nid=%d, data=%f>",
					msg.(MessageIncEvalUpdate).from,
					msg.(MessageIncEvalUpdate).from,
					msg.(MessageIncEvalUpdate).round,
					msg.(MessageIncEvalUpdate).nid,
					msg.(MessageIncEvalUpdate).data)

				// TODO: IMPLEMENT IncEval & send messages and incremental updatesが0の時にnotifyinactivをmasterに送信
				// TODO: Incremntal updateを実行するたびにisinactiveをfalseにする
			case msg := <- w.Receive(opcodeNotifyInactive):
				log.Info().Msgf("received inactive notification from peer %d", msg.(MessageNotifyInactive).from)
				flag := true
				for _, status := range w.inactiveMap {
					flag = flag && status
				}
				if flag {
					for _, p := range w.peers {
						if err := w.SendMessage(p, MessageTerminate{
							from:      w.id,
							debugText: "Terminate",
						}); err != nil {
							panic(err)
						}
					}
				}
			case msg := <- w.Receive(opcodeTerminate):
				log.Info().Msgf("received terminate message from peer %d", msg.(MessageTerminate).from)
				if w.isInactive {
					if err := w.SendMessage(w.master, MessageTerminateACK{from:w.id}); err != nil {
						panic(err)
					}
				}
			case msg := <- w.Receive(opcodeTerminateACK):
				log.Info().Msgf("received terminate message from peer %d", msg.(MessageTerminateACK).from)
				for _, p := range w.peers {
					if err := w.SendMessage(p, MessageAssembleRequest{
						from:      w.id,
						debugText: "ASSEMBLE",
					}); err != nil {
						panic(err)
					}
				}
			case msg := <- w.Receive(opcodeAssembleRequest):
				log.Info().Msgf("received Assemble request from peer %d", msg.(MessageAssembleRequest).from)
				if err := w.SendMessage(w.master, MessageAssembleResponse{from:w.id, result:w.shortestPath.Result()}); err != nil {
					panic(err)
				}
			case msg := <- w.Receive(opcodeAssembleResponse):
				log.Info().Msgf("received Assemble response from peer %d, result: %+v",
					msg.(MessageAssembleResponse).from, msg.(MessageAssembleResponse).result)
			}
		}
	}()
}

func (w *Worker) messageSender() {
	for {
		var cmd sendHandle
		select {
		case cmd = <-w.sendQueue:
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
	case err = <-cmd.result:
		return err
	}
}
func (w *Worker) SendMessageAsync(to *Peer, message Message) <-chan error {
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

func (w *Worker) Receive(opcode Opcode) <-chan Message {
	c, _ := w.recvQueue.LoadOrStore(opcode, receiveHandle{
		hub:  make(chan Message),
		lock: make(chan struct{}, 1),
	})
	return c.(receiveHandle).hub
}

// TODO: ID交換
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
		peer.init() // go peer.messageReceiver()
		// TODO: ここにいろいろ書けばpeerごとに実行される
		if err := w.SendMessage(peer, MessageHello{from: w.id}); err != nil {
			panic(err)
		}
		select {
		case msg := <-w.Receive(opcodeHello):
			peerID := msg.(MessageHello).from
			w.peers[peerID] = peer
			w.inactiveMap[peerID] = false
		}
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
	if err := w.SendMessage(peer, MessageHello{from: w.id}); err != nil {
		panic(err)
	}
	select {
	case msg := <-w.Receive(opcodeHello):
		peerID := msg.(MessageHello).from
		w.peers[peerID] = peer
		w.inactiveMap[peerID] = false
	}
	return peer, nil
}

func (w *Worker) Disconnect(peer *Peer) {
	id := peer.id
	delete(w.peers, id)
	peer.Disconnect()
}
func (w *Worker) DisconnectAsync(peer *Peer) <-chan struct{} {
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
func (w *Worker) IsInactive() bool {
	return w.isInactive
}

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
	"sync/atomic"
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
	inactiveMap           map[uint64]bool
	terminateMap          map[uint64]bool
	assembleMap map[uint64]interface{}
	kill                  chan chan struct{}
	killOnce              uint32
}

func NewWorker(id uint64, host string, port uint16, isMaster bool) (*Worker, error) {
	tcp := transport.NewTCP()
	listener, err := tcp.Listen(host, port)
	if err != nil {
		return nil, errors.Errorf("failed to create listener for peers on port %d", port)
	}
	var (
		weightedDirectedGraph *graph.WeightedDirectedGraph
		possessionTable       map[int64]graph.Uint64Set
		shortestPath          *graph.ShortestPath
	)
	if isMaster {
		worker := Worker{
			id:                    id,
			transport:             tcp,
			listener:              listener,
			host:                  host,
			port:                  port,
			peers:                 make(map[uint64]*Peer),
			sendQueue:             make(chan sendHandle, 128),
			recvQueue:             sync.Map{},
			round:                 0,
			weightedDirectedGraph: weightedDirectedGraph,
			shortestPath:          shortestPath,
			possessionTable:       possessionTable,
			isInactive:            false,
			inactiveMap:           make(map[uint64]bool),
			terminateMap:          make(map[uint64]bool),
			assembleMap: make(map[uint64]interface{}),
			kill:                  make(chan chan struct{}, 1),
			killOnce:              0,
		}
		worker.init() // go worker.messageSender()
		return &worker, nil
	} else {
		address := fmt.Sprintf("%s:%d", host, port)
		weightedDirectedGraph, possessionTable = graph.NewWeightedDirectedGraphFromCSV(address, 0.0, math.Inf(1))
		log.Info().Msgf("nodes: %+v", weightedDirectedGraph.Nodes())
		shortestPath = graph.NewShortestFrom(graph.NewNode(0), weightedDirectedGraph.Nodes())
		if id == 0 {
			shortestPath.SetZero()
		}
		worker := Worker{
			id:                    id,
			transport:             tcp,
			listener:              listener,
			host:                  host,
			port:                  port,
			peers:                 make(map[uint64]*Peer),
			sendQueue:             make(chan sendHandle, 128),
			recvQueue:             sync.Map{},
			round:                 0,
			weightedDirectedGraph: weightedDirectedGraph,
			shortestPath:          shortestPath,
			possessionTable:       possessionTable,
			isInactive:            false,
			inactiveMap:           make(map[uint64]bool),
			terminateMap:          make(map[uint64]bool),
			assembleMap: make(map[uint64]interface{}),
			kill:                  make(chan chan struct{}, 1),
			killOnce:              0,
		}
		worker.init() // go worker.messageSender()
		return &worker, nil
	}
}

// opcodes
var (
	opcodeHello            Opcode
	opcodePEvalRequest     Opcode
	opcodeIncEvalUpdate    Opcode
	opcodeNotifyInactive   Opcode
	opcodeTerminate        Opcode
	opcodeTerminateACK     Opcode
	opcodeAssembleRequest  Opcode
	opcodeAssembleResponse Opcode
)

// register messages and spawn message sender
func (w *Worker) init() {
	opcodeHello = RegisterMessage(NextAvailableOpcode(), (*MessageHello)(nil))
	opcodePEvalRequest = RegisterMessage(NextAvailableOpcode(), (*MessagePEvalRequest)(nil))
	opcodeIncEvalUpdate = RegisterMessage(NextAvailableOpcode(), (*MessageIncEvalUpdate)(nil))
	opcodeNotifyInactive = RegisterMessage(NextAvailableOpcode(), (*MessageNotifyInactive)(nil))
	opcodeTerminate = RegisterMessage(NextAvailableOpcode(), (*MessageTerminate)(nil))
	opcodeTerminateACK = RegisterMessage(NextAvailableOpcode(), (*MessageTerminateACK)(nil))
	opcodeAssembleRequest = RegisterMessage(NextAvailableOpcode(), (*MessageAssembleRequest)(nil))
	opcodeAssembleResponse = RegisterMessage(NextAvailableOpcode(), (*MessageAssembleResponse)(nil))
	w.recvQueue.Store(opcodeHello, receiveHandle{
		hub:  make(chan Message, 128),
		lock: make(chan struct{}, 1),
	})
	w.recvQueue.Store(opcodePEvalRequest, receiveHandle{
		hub:  make(chan Message, 128),
		lock: make(chan struct{}, 1),
	})
	w.recvQueue.Store(opcodeNotifyInactive, receiveHandle{
		hub:  make(chan Message, 128),
		lock: make(chan struct{}, 1),
	})
	w.recvQueue.Store(opcodeTerminate, receiveHandle{
		hub:  make(chan Message, 128),
		lock: make(chan struct{}, 1),
	})
	w.recvQueue.Store(opcodeTerminateACK, receiveHandle{
		hub:  make(chan Message, 128),
		lock: make(chan struct{}, 1),
	})
	w.recvQueue.Store(opcodeAssembleRequest, receiveHandle{
		hub:  make(chan Message, 128),
		lock: make(chan struct{}, 1),
	})
	w.recvQueue.Store(opcodeAssembleResponse, receiveHandle{
		hub:  make(chan Message, 128),
		lock: make(chan struct{}, 1),
	})
	w.recvQueue.Store(opcodeIncEvalUpdate, receiveHandle{
		hub:  make(chan Message, 128),
		lock: make(chan struct{}, 1),
	})
	go w.messageSender()
	log.Info().Msgf("possessionTable: %+v", w.possessionTable)
	// TODO: ここで全typeのメッセージを待つのがダメっぽい
	//go func() {
	//	for {
	//		select {
	//		case msg := <-w.Receive(opcodePEvalRequest):
	//			atomic.AddUint64(&w.round, 1)
	//			log.Info().Msgf("received PEval request From peer %d", msg.(MessagePEvalRequest).From)
	//			master := w.peers[msg.(MessagePEvalRequest).From]
	//			w.master = master
	//			log.Info().Msgf("set peer %d as master", master.id)
	//			if err := w.SendMessage(master, MessagePEvalResponse{from: w.id, debugText: "OK"}); err != nil {
	//				panic(err)
	//			}
	//			// TODO: グラフを読み込んだ時点で，F.I/F.O/F.I'/F.O'を計算しておくべき
	//			graph.PEvalDijkstra(w.weightedDirectedGraph, w.shortestPath)
	//			if len(w.shortestPath.UpdatedNodeIDs()) == 0 {
	//				w.isInactive = true
	//				continue
	//			}
	//			log.Info().Msgf("PEval updates: UpdatedNodeIDs=%+v", w.shortestPath.UpdatedNodeIDs())
	//			for _, nid := range w.shortestPath.UpdatedNodeIDs() {
	//				owners := w.possessionTable[nid]
	//				for ownerID := range owners {
	//					if ownerID != w.master.id && ownerID != w.id {
	//						if err := w.SendMessage(w.peers[ownerID], MessageIncEvalUpdate{
	//							from:  w.id,
	//							round: w.round,
	//							nid:   nid,
	//							data:  w.shortestPath.DistOf(nid),
	//						}); err != nil {
	//							panic(err)
	//						}
	//					}
	//				}
	//			}
	//		case msg := <-w.Receive(opcodePEvalResponse):
	//			log.Info().Msgf("received PEval response From peer %d", msg.(MessagePEvalResponse).from)
	//		case msg := <-w.Receive(opcodeIncEvalUpdate):
	//			w.isInactive = false
	//			atomic.AddUint64(&w.round, 1)
	//			log.Info().Msgf("received IncEval request From peer %d: <From=%d, round=%d, nid=%d, data=%f>",
	//				msg.(MessageIncEvalUpdate).from,
	//				msg.(MessageIncEvalUpdate).from,
	//				msg.(MessageIncEvalUpdate).round,
	//				msg.(MessageIncEvalUpdate).nid,
	//				msg.(MessageIncEvalUpdate).data)
	//			//updates := make([]graph.DistanceNode, len(w.Receive(opcodeIncEvalUpdate)))
	//			//i := 0
	//			//for msg := range w.Receive(opcodeIncEvalUpdate) {
	//			//	updates[i] = graph.NewDistanceNode(msg.(MessageIncEvalUpdate).nid, msg.(MessageIncEvalUpdate).data)
	//			//	i++
	//			//}
	//			graph.IncEvalDijkstra([]graph.DistanceNode{graph.NewDistanceNode(msg.(MessageIncEvalUpdate).nid, msg.(MessageIncEvalUpdate).data)}, w.weightedDirectedGraph, w.shortestPath)
	//			log.Info().Msgf("IncEval updates: UpdatedNodeIDs=%+v", w.shortestPath.UpdatedNodeIDs())
	//			for _, nid := range w.shortestPath.UpdatedNodeIDs() {
	//				owners := w.possessionTable[nid]
	//				log.Info().Msgf("inc update : owners=%+v", owners)
	//				for ownerID := range owners {
	//					if ownerID != w.master.id && ownerID != w.id {
	//						log.Info().Msgf("owner=%+v", w.peers[ownerID])
	//						if err := w.SendMessage(w.peers[ownerID], MessageIncEvalUpdate{
	//							from:  w.id,
	//							round: w.round,
	//							nid:   nid,
	//							data:  w.shortestPath.DistOf(nid),
	//						}); err != nil {
	//							panic(err)
	//						}
	//					}
	//				}
	//			}
	//
	//			if len(w.Receive(opcodeIncEvalUpdate)) == 0 {
	//				w.isInactive = true
	//				if err := w.SendMessage(w.master, MessageNotifyInactive{from: w.id}); err != nil {
	//					panic(err)
	//				}
	//			}
	//			//if len(w.shortestPath.UpdatedNodeIDs()) == 0 {
	//			//	w.isInactive = true
	//			//	if err := w.SendMessage(w.master, MessageNotifyInactive{from:w.id}); err != nil {
	//			//		panic(err)
	//			//	}
	//			//	continue
	//			//} else {
	//			//	for _, nid := range w.shortestPath.UpdatedNodeIDs() {
	//			//		owners := w.possessionTable[nid]
	//			//		log.Info().Msgf("inc update : owners=%+v", owners)
	//			//		for ownerID := range owners {
	//			//			if ownerID != w.master.id && ownerID != w.id {
	//			//				log.Info().Msgf("owner=%+v", w.peers[ownerID])
	//			//				if err := w.SendMessage(w.peers[ownerID], MessageIncEvalUpdate{
	//			//					from:  w.id,
	//			//					round: w.round,
	//			//					nid:   nid,
	//			//					data:  w.shortestPath.DistOf(nid),
	//			//				}); err != nil {
	//			//					panic(err)
	//			//				}
	//			//			}
	//			//		}
	//			//	}
	//			//}
	//
	//			// TODO: IMPLEMENT IncEval & send messages and incremental updatesが0の時にnotifyinactivをmasterに送信
	//			// TODO: Incremntal updateを実行するたびにisinactiveをfalseにする
	//		case msg := <-w.Receive(opcodeNotifyInactive):
	//			log.Info().Msgf("received inactive notification From peer %d", msg.(MessageNotifyInactive).from)
	//			w.inactiveMap[msg.(MessageNotifyInactive).from] = true
	//			flag := true
	//			for _, status := range w.inactiveMap {
	//				flag = flag && status
	//			}
	//			if flag {
	//				for _, p := range w.peers {
	//					if err := w.SendMessage(p, MessageTerminate{
	//						from:      w.id,
	//						debugText: "Terminate",
	//					}); err != nil {
	//						panic(err)
	//					}
	//				}
	//			}
	//		case msg := <-w.Receive(opcodeTerminate):
	//			log.Info().Msgf("received terminate message From peer %d", msg.(MessageTerminate).from)
	//			if w.isInactive {
	//				if err := w.SendMessage(w.master, MessageTerminateACK{from: w.id}); err != nil {
	//					panic(err)
	//				}
	//			}
	//		case msg := <-w.Receive(opcodeTerminateACK):
	//			log.Info().Msgf("received terminate ack message From peer %d", msg.(MessageTerminateACK).from)
	//			w.terminateMap[msg.(MessageTerminateACK).from] = true
	//			flag := true
	//			for _, status := range w.terminateMap {
	//				flag = flag && status
	//			}
	//			if flag {
	//				for _, p := range w.peers {
	//					if err := w.SendMessage(p, MessageAssembleRequest{
	//						from:      w.id,
	//						debugText: "ASSEMBLE",
	//					}); err != nil {
	//						panic(err)
	//					}
	//				}
	//			}
	//
	//		case msg := <-w.Receive(opcodeAssembleRequest):
	//			log.Info().Msgf("received Assemble request From peer %d", msg.(MessageAssembleRequest).from)
	//			if err := w.SendMessage(w.master, MessageAssembleResponse{from: w.id, result: w.shortestPath.Result()}); err != nil {
	//				panic(err)
	//			}
	//		case msg := <-w.Receive(opcodeAssembleResponse):
	//			log.Info().Msgf("received Assemble response From peer %d, result: %+v",
	//				msg.(MessageAssembleResponse).from, msg.(MessageAssembleResponse).result)
	//		}
	//	}
	//}()
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
		log.Info().Msgf("accept: %s", conn.RemoteAddr().String())
		peer := NewPeer(w, conn)
		peer.init() // go peer.messageReceiver()
		// TODO: ここにいろいろ書けばpeerごとに実行される
		if err := w.SendMessage(peer, MessageHello{from: w.id}); err != nil {
			panic(err)
		}
		select {
		case msg := <-w.Receive(opcodeHello):
			peerID := msg.(MessageHello).from
			peer.id = peerID
			w.peers[peerID] = peer
			w.inactiveMap[peerID] = false
			w.terminateMap[peerID] = false
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
		peer.id = peerID
		w.peers[peerID] = peer
		w.inactiveMap[peerID] = false
		w.terminateMap[peerID] = false
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

func (w *Worker) RunAsWorker() {
	msg := <- w.Receive(opcodePEvalRequest)
	log.Info().Msgf("recv PEval request from master id=%d", msg.(MessagePEvalRequest).From)
	atomic.AddUint64(&w.round, 1)
	master := w.peers[msg.(MessagePEvalRequest).From]
	w.master = master
	log.Info().Msgf("set peer %d as master", master.id)
	log.Info().Msg("start PEval")
	graph.PEvalDijkstra(w.weightedDirectedGraph, w.shortestPath)
	if len(w.shortestPath.UpdatedNodeIDs()) == 0 {
		w.isInactive = true
	}
	log.Info().Msgf("PEval updates: UpdatedNodeIDs=%+v", w.shortestPath.UpdatedNodeIDs())
	for _, nid := range w.shortestPath.UpdatedNodeIDs() {
		owners := w.possessionTable[nid]
		for ownerID := range owners {
			if ownerID != w.master.id && ownerID != w.id {
				if err := w.SendMessage(w.peers[ownerID], MessageIncEvalUpdate{
					from:  w.id,
					round: w.round,
					nid:   nid,
					data:  w.shortestPath.DistOf(nid),
				}); err != nil {
					panic(err)
				}
			}
		}
	}

	var assembleReq Message

	for {
		select {
		case msg := <-w.Receive(opcodeIncEvalUpdate):
			w.isInactive = false
			atomic.AddUint64(&w.round, 1)
			log.Info().Msgf("received IncEval request From peer %d: <From=%d, round=%d, nid=%d, data=%f>",
				msg.(MessageIncEvalUpdate).from,
				msg.(MessageIncEvalUpdate).from,
				msg.(MessageIncEvalUpdate).round,
				msg.(MessageIncEvalUpdate).nid,
				msg.(MessageIncEvalUpdate).data)
			//// get all updates from buffer
			updates := make([]graph.DistanceNode, len(w.Receive(opcodeIncEvalUpdate)) + 1)
			updates[0] = graph.NewDistanceNode(msg.(MessageIncEvalUpdate).nid, msg.(MessageIncEvalUpdate).data)
			for i := 1; i < len(w.Receive(opcodeIncEvalUpdate)); i++ {
				updates[i] = graph.NewDistanceNode(msg.(MessageIncEvalUpdate).nid, msg.(MessageIncEvalUpdate).data)
			}
			log.Info().Msgf("updates: %+v", updates)

			// incremental evaluation
			graph.IncEvalDijkstra(updates, w.weightedDirectedGraph, w.shortestPath)
			log.Info().Msgf("IncEval updates: UpdatedNodeIDs=%+v", w.shortestPath.UpdatedNodeIDs())

			// send updates to peers
			for _, nid := range w.shortestPath.UpdatedNodeIDs() {
				owners := w.possessionTable[nid]
				log.Info().Msgf("inc update : owners=%+v", owners)
				for ownerID := range owners {
					if ownerID != w.master.id && ownerID != w.id {
						log.Info().Msgf("owner=%+v", w.peers[ownerID])
						if err := w.SendMessage(w.peers[ownerID], MessageIncEvalUpdate{
							from:  w.id,
							round: w.round,
							nid:   nid,
							data:  w.shortestPath.DistOf(nid),
						}); err != nil {
							panic(err)
						}
					}
				}
			}

			// if message buffer is empty, set me inactive
			if len(w.Receive(opcodeIncEvalUpdate)) == 0 {
				w.isInactive = true
				if err := w.SendMessage(w.master, MessageNotifyInactive{from: w.id}); err != nil {
					panic(err)
				}
			}
		case <-w.Receive(opcodeTerminate):
			// TODO: check my inactive flag and send TerminateACK to master
			log.Info().Msg("receive terminate")
			if w.isInactive && len(w.Receive(opcodeIncEvalUpdate)) == 0 {
				if err := w.SendMessage(w.master, MessageTerminateACK{from:w.id}); err != nil {
					log.Info().Msgf("error: can not send terminate ack to master")
				}
				log.Info().Msg("send terminate ack to master")
			} else {
				continue
			}
		case assembleReq = <-w.Receive(opcodeAssembleRequest):
			goto ASSEMBLE
		}
	}

ASSEMBLE:
	log.Info().Msgf("received Assemble request From peer %d", assembleReq.(MessageAssembleRequest).from)
	if err := w.SendMessage(w.master, MessageAssembleResponse{from: w.id, result: w.shortestPath.Result()}); err != nil {
		panic(err)
	}
	log.Info().Msgf("send assemble response: %+v", MessageAssembleResponse{from: w.id, result: w.shortestPath.Result()})
	log.Info().Msg("Done!")
}

func (w *Worker) RunAsMaster() {
	log.Info().Msg("send PEval Request")
	for _, peer := range w.peers {
		if err := w.SendMessage(peer, MessagePEvalRequest{From:w.id}); err != nil {
			panic(err)
		}
	}
	for {
		select {
		case msg := <-w.Receive(opcodeNotifyInactive):
			log.Info().Msgf("received inactive notification From peer %d", msg.(MessageNotifyInactive).from)
			w.inactiveMap[msg.(MessageNotifyInactive).from] = true
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
				log.Info().Msg("broadcast terminate")
			}
		case msg := <-w.Receive(opcodeTerminateACK):
			log.Info().Msgf("received terminate ack message From peer %d", msg.(MessageTerminateACK).from)
			w.terminateMap[msg.(MessageTerminateACK).from] = true
			flag := true
			for _, status := range w.terminateMap {
				flag = flag && status
			}
			if flag {
				goto ASSEMBLE
			}
		}
	}
ASSEMBLE:
	for _, p := range w.peers {
		if err := w.SendMessage(p, MessageAssembleRequest{
			from:      w.id,
			debugText: "ASSEMBLE",
		}); err != nil {
			panic(err)
		}
	}
	log.Info().Msg("send assemble request")
	for {
		select {
		case msg := <-w.Receive(opcodeAssembleResponse):
			log.Info().Msgf("received Assemble response From peer %d, result: %+v",
				msg.(MessageAssembleResponse).from, msg.(MessageAssembleResponse).result)
			// TODO: ここから抜ける
		}
	}
}
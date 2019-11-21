package main

import (
	"bufio"
	"flag"
	"github.com/pkg/errors"
	"github.com/zakimal/aap"
	"github.com/zakimal/aap/graph"
	"github.com/zakimal/aap/log"
	"github.com/zakimal/aap/payload"
	"os"
	"strings"
)

var (
	opcodeIncrementalUpdate aap.Opcode
)

type IncrementalUpdate struct {
	workerAddr string
	round      uint64
	nodeID     int64
	data       float64
}

func (IncrementalUpdate) Read(reader payload.Reader) (aap.Message, error) {
	workerAddr, err := reader.ReadString()
	if err != nil {
		return nil, errors.Wrap(err, "aap: failed to read `workerAddr` from `IncrementalUpdate` message")
	}
	round, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "aap: failed to read `round` from `IncrementalUpdate` message")
	}
	nodeID, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "aap: failed to read `nodeID` from `IncrementalUpdate` message")
	}
	data, err := reader.ReadFloat64()
	if err != nil {
		return nil, errors.Wrap(err, "aap: failed to read `data` from `IncrementalUpdate` message")
	}
	return IncrementalUpdate{
		workerAddr: workerAddr,
		round:      round,
		nodeID:     int64(nodeID),
		data:       data,
	}, nil
}
func (i IncrementalUpdate) Write() []byte {
	return payload.NewWriter(nil).
		WriteString(i.workerAddr).
		WriteUint64(i.round).
		WriteUint64(uint64(i.nodeID)).
		WriteFloat64(i.data).
		Bytes()
}

func main() {
	// Setting flags...
	// $ go run cmd/catalogue/main.go -host=127.0.0.2 -port=3001 127.0.0.1:3001.txt 127.0.0.1:3003
	hostFlag := flag.String("host", "127.0.0.1", "host address to listen for peer on")
	portFlag := flag.Uint("port", 3000, "port to listen for peer on")
	workerIDFlag := flag.Int64("id", -1, "worker ID")
	// Parsing flags...
	flag.Parse()

	if *workerIDFlag < 0 {
		panic("workerID do not be negative")
	}

	setup()

	// Start worker...
	worker, err := aap.NewWorker(*workerIDFlag, *hostFlag, uint16(*portFlag))
	if err != nil {
		panic(err)
	}
	defer worker.Kill()

	go worker.Listen()
	log.Info().Msgf("Listening for peers at %s", worker.Address())

	go func() {
		for {
			select {
			case update := <-worker.ReceiveMessage(opcodeIncrementalUpdate):
				updateNodes := make([]aap.DistanceNode, 0)
				updateNodes = append(updateNodes, aap.NewDistanceNode(graph.NewNode(update.(IncrementalUpdate).nodeID, update.(IncrementalUpdate).workerAddr), update.(IncrementalUpdate).data))
				shortestPath := worker.IncEvalDijkstraFrom(updateNodes)
				for n := range shortestPath.ChangedNodeID() {
					peer := worker.Peer(worker.OwnerTable()[int64(n)])
					worker.SendMessageAsync(peer, IncrementalUpdate{
						workerAddr: worker.OwnerTable()[int64(n)],
						round:      worker.Round(),
						nodeID:     int64(n),
						data:       shortestPath.DistOf(int64(n)),
					})
				}
			}
		}
	}()
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		cmd := strings.Split(input, " ")
		switch cmd[0] {
		case "dial":
			addrs := cmd[1:]
			// Establish connections with peers at given addresses...
			if len(addrs) > 0 {
				for _, address := range addrs {
					// Dialing a peer...
					peer, err := worker.Dial(address)
					if err != nil {
						panic(err)
					}
					worker.SetPeer(peer)
				}
			}
		case "dijkstra":
			shortestPath := worker.DijkstraFrom(graph.NewNode(0, worker.OwnerTable()[0]))
			log.Printf("shortest path: %+v", shortestPath)
		case "peval":
			shortestPath := worker.PEvalDijkstraFrom(graph.NewNode(0, worker.OwnerTable()[0]))
			log.Printf("shortest path: %+v", shortestPath)
			for n := range shortestPath.ChangedNodeID() {
				peer := worker.Peer(worker.OwnerTable()[int64(n)])
				log.Printf("%+v", peer)
				worker.SendMessageAsync(peer, IncrementalUpdate{
					workerAddr: worker.OwnerTable()[int64(n)],
					round:      worker.Round(),
					nodeID:     int64(n),
					data:       shortestPath.DistOf(int64(n)),
				})
			}
		}
	}
}

func setup() {
	opcodeIncrementalUpdate = aap.RegisterMessage(aap.NextAvailableOpcode(), (*IncrementalUpdate)(nil))
}

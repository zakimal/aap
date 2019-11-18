package main

import (
	"bufio"
	"flag"
	"github.com/pkg/errors"
	"github.com/zakimal/aap"
	"github.com/zakimal/aap/log"
	"github.com/zakimal/aap/payload"

	"os"
	"strings"
)

// Define message type and opcode
var (
	opcodeChat aap.Opcode
	opcodeAAPMessage aap.Opcode
)

type chatMessage struct {
	text string
}
func (m chatMessage) Read(reader payload.Reader) (aap.Message, error) {
	text, err := reader.ReadString()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read content of `chatMessage`")
	}
	return chatMessage{text: text}, nil
}
func (m chatMessage) Write() []byte {
	return payload.NewWriter(nil).WriteString(m.text).Bytes()
}

type aapMessage struct {
	workerID int64
	round int64
	nodeID int64
	data float64
}
func (m aapMessage) Read(reader payload.Reader) (aap.Message, error) {
	workerID, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read workerID of aapMessage")
	}
	round, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read round of aapMessage")
	}
	nodeID, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read nodeID of aapMessage")
	}
	data, err := reader.ReadFloat64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read data of aapMessage")
	}
	return aapMessage{
		workerID: int64(workerID),
		round:    int64(round),
		nodeID:   int64(nodeID),
		data:     data,
	}, nil
}
func (m aapMessage) Write() []byte {
	return payload.NewWriter(nil).
		WriteUint64(uint64(m.workerID)).
		WriteUint64(uint64(m.round)).
		WriteUint64(uint64(m.nodeID)).
		WriteFloat64(m.data).
		Bytes()
}

func main() {
	// Setting flags...
	// $ go run cmd/catalogue/main.go -host=127.0.0.2 -port=3001 1.txt 127.0.0.1:3003
	hostFlag := flag.String("host", "127.0.0.1", "host address to listen for peer on")
	portFlag := flag.Uint("port", 3000, "port to listen for peer on")
	workerIDFlag := flag.Int64("id", -1, "worker ID")
	// Parsing flags...
	flag.Parse()

	if *workerIDFlag < 0 {
		panic("workerID do not be negative")
	}

	// Registering message types...
	opcodeChat = aap.RegisterMessage(aap.NextAvailableOpcode(), (*chatMessage)(nil))

	// Start worker...
	worker, err := aap.NewWorker(int64(*workerIDFlag), *hostFlag, uint16(*portFlag))
	if err != nil {
		panic(err)
	}
	defer worker.Kill()

	go worker.Listen()
	log.Info().Msgf("Listening for peers at %s", worker.Address())

	go func() {
		for {
			select {
			case msg := <- worker.Receive(opcodeChat):
				log.Info().Msgf("recv: %+v", msg.(chatMessage).text)
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
		case "peval":
		}
	}
}

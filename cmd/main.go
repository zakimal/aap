package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/zakimal/aap"
	"github.com/zakimal/aap/log"

	"os"
	"strings"
)

var opcodeChat aap.Opcode

func main() {
	// Setting flags...
	// $ go run cmd/catalogue/main.go -host=127.0.0.2 -port=3001 127.0.0.1:3001 127.0.0.1:3003
	hostFlag := flag.String("host", "127.0.0.1", "host address to listen for peer on")
	portFlag := flag.Uint("port", 3000, "port to listen for peer on")

	// Parsing flags...
	flag.Parse()

	workerID := fmt.Sprintf("%s:%d", *hostFlag, *portFlag)

	opcodeChat = aap.RegisterMessage(aap.NextAvailableOpcode(), (*aap.ChatMessage)(nil))

	worker, err := aap.NewWorker(workerID, *hostFlag, uint16(*portFlag))
	if err != nil {
		panic(err)
	}
	defer worker.Kill()

	go worker.Listen()
	log.Info().Msgf("Listening for peers at %s", worker.Address())

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
					worker.Peers = append(worker.Peers, peer)
				}
			}
		case "broadcast":
			txt := cmd[1]
			worker.Broadcast(aap.ChatMessage{Text:txt})
		}
	}
}
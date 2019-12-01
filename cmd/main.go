package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/zakimal/aap"
	"github.com/zakimal/aap/log"
	"os"
	"time"
)

func main() {
	hostFlag := flag.String("host", "127.0.0.1", "host address to listen for peer on")
	portFlag := flag.Uint("port", 3000, "port to listen for peer on")
	idFlag := flag.Uint64("id", 0, "worker ID")
	// TODO: isMaster flag
	masterFlag := flag.Bool("master", false, "are you master?")

	flag.Parse()
	worker, err := aap.NewWorker(*idFlag, *hostFlag, uint16(*portFlag), *masterFlag)
	if err != nil {
		panic(err)
	}
	log.Info().Msgf("start worker: id = %d", worker.ID())

	log.Info().Msgf("Listening for peers on %s", worker.Address())
	go worker.Listen()

	time.Sleep(3 * time.Second)

	// TODO: read config and dial peers
	config, err := os.Open(fmt.Sprintf("/Users/zak/go/src/github.com/zakimal/aap/cmd/config/%s:%d.txt", *hostFlag, *portFlag))
	if err != nil {
		panic(err)
	}
	defer config.Close()

	scanner := bufio.NewScanner(config)
	for scanner.Scan() {
		peer, err := worker.Dial(scanner.Text())
		if err != nil {
			panic(err)
		}
		log.Info().Msgf("Connected with peer %+v", peer.ID())
	}

	time.Sleep(3 * time.Second)

	if *masterFlag {
		worker.RunAsMaster()
	} else {
		worker.RunAsWorker()
	}

	//scanner := bufio.NewScanner(os.Stdin)
	//for scanner.Scan() {
	//	cmds := scanner.Text()
	//	cmd := strings.Split(cmds, " ")
	//	args := cmd[1:]
	//	switch cmd[0] {
	//	case "dial":
	//		if len(args) == 0 {
	//			log.Info().Msg("Usage: dial xxx.xxx.xxx.xxx:pppp")
	//			continue
	//		}
	//		// Dialing them...
	//		for _, address := range args {
	//			peer, err := worker.Dial(address)
	//			if err != nil {
	//				panic(err)
	//			}
	//			log.Info().Msgf("Connected with peer %+v", peer.ID())
	//		}
	//	case "peers":
	//		log.Info().Msgf("Peers: %+v", worker.Peers())
	//	case "peval":
	//		for _, peer := range worker.Peers() {
	//			if err := worker.SendMessage(peer, aap.MessagePEvalRequest{From: worker.ID()}); err != nil {
	//				panic(err)
	//			}
	//		}
	//	case "path":
	//		shortestPath := worker.ShortestPath()
	//		log.Info().Msgf("shortest path: %+v", shortestPath.Result())
	//	case "dijkstra":
	//		shortestPath := graph.Dijkstra()
	//		log.Info().Msgf("shortestPath: %+v", shortestPath.Result())
	//	}
	//}
}

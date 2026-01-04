package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"razpravljalnica/orchestrator"
	"syscall"
)

func main() {
	nodeId := flag.String("id", "orch1", "orchestrator node ID")
	grpcPort := flag.Int("p", 8000, "gRPC port")
	raftPort := flag.Int("rp", 7000, "Raft port")
	raftDir := flag.String("dir", "./raft-data", "Raft data directory")
	bootstrap := flag.Bool("bootstrap", false, "bootstrap new cluster")
	joinAddr := flag.String("join", "", "address of leader to join")
	flag.Parse()

	grpcAddr := fmt.Sprintf("localhost:%d", *grpcPort)
	raftAddr := fmt.Sprintf("localhost:%d", *raftPort)
	dataDir := fmt.Sprintf("%s/%s", *raftDir, *nodeId)

	orch := orchestrator.NewOrchestrator(*nodeId, grpcAddr, raftAddr, dataDir)

	if err := orch.Start(*bootstrap); err != nil {
		panic(err)
	}

	// Ce cluster ze obstaja
	if *joinAddr != "" {
		// TODO: se treba implementirat
		fmt.Println("Se treba implementirat")
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("Shutting down...")
}

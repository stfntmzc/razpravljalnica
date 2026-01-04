package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"razpravljalnica/orchestrator"
	pb "razpravljalnica/proto"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	nodeId := flag.String("id", "orch1", "orchestrator node ID")
	grpcPort := flag.Int("p", 8000, "gRPC port")
	raftPort := flag.Int("rp", 7000, "Raft port")
	raftDir := flag.String("dir", "./raft-data", "Raft data directory")
	bootstrap := flag.Bool("bootstrap", false, "bootstrap new cluster")
	joinAddr := flag.String("join", "", "gRPC address of leader to join")
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
		time.Sleep(1 * time.Second)

		conn, err := grpc.NewClient(*joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		client := pb.NewOrchestratorClient(conn)
		resp, err := client.JoinCluster(context.Background(), &pb.JoinClusterRequest{
			NodeId:      *nodeId,
			RaftAddress: raftAddr,
		})
		if err != nil {
			panic(err)
		}
		if !resp.Success {
			panic(fmt.Sprintf("failed to join: %s", resp.Error))
		}
		fmt.Printf("Successfully joined cluster via %s\n", *joinAddr)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("Shutting down...")
}

package orchestrator

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	pb "razpravljalnica/proto"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type NodeInfo struct {
	Id          string
	Address     string
	Role        string
	NextAddress string
	PrevAddress string
	Reconfigure bool
}

type Orchestrator struct {
	pb.UnimplementedOrchestratorServer

	mu         sync.RWMutex
	nodes      map[string]*NodeInfo // nodeId -> info
	chainOrder []string             // ordered list of node IDs
	nodeHealth map[string]time.Time // nodeId -> last heartbeat
	nodeSubs   map[string]int32     // nodeId -> subscriber count
}

func NewOrchestrator() *Orchestrator {
	return &Orchestrator{
		nodes:      make(map[string]*NodeInfo),
		chainOrder: []string{},
		nodeHealth: make(map[string]time.Time),
		nodeSubs:   make(map[string]int32),
	}
}

func (o *Orchestrator) Start(address string) {
	// Start health monitor
	go o.monitorHealth()

	// Start gRPC server
	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterOrchestratorServer(grpcServer, o)

	fmt.Printf("Orchestrator listening on %s\n", address)
	grpcServer.Serve(listener)
}

func (o *Orchestrator) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	nodeId := fmt.Sprintf("node-%d", len(o.nodes)+1)
	var role, nextAddr, prevAddr string

	if len(o.chainOrder) == 0 {

		role = "head"
	} else {

		oldTailId := o.chainOrder[len(o.chainOrder)-1]
		oldTail := o.nodes[oldTailId]

		oldTail.Role = "middle"
		oldTail.NextAddress = req.Address
		oldTail.Reconfigure = true

		role = "tail"
		prevAddr = oldTail.Address
	}

	node := &NodeInfo{
		Id:          nodeId,
		Address:     req.Address,
		Role:        role,
		NextAddress: nextAddr,
		PrevAddress: prevAddr,
	}

	o.nodes[nodeId] = node
	o.chainOrder = append(o.chainOrder, nodeId)
	o.nodeHealth[nodeId] = time.Now()

	fmt.Printf("Node registered: %s (%s) at %s\n", nodeId, role, req.Address)

	return &pb.RegisterNodeResponse{
		NodeId:      nodeId,
		Role:        role,
		NextAddress: nextAddr,
		PrevAddress: prevAddr,
	}, nil
}

func (o *Orchestrator) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// DEBUG: fmt.Printf("Heartbeat received from: %s\n", req.NodeId)

	o.mu.Lock()
	defer o.mu.Unlock()

	o.nodeHealth[req.NodeId] = time.Now()
	o.nodeSubs[req.NodeId] = req.SubscriberCount

	node := o.nodes[req.NodeId]
	if node == nil {
		return &pb.HeartbeatResponse{}, nil
	}

	resp := &pb.HeartbeatResponse{
		Reconfigure: node.Reconfigure,
		NewNext:     node.NextAddress,
		NewPrev:     node.PrevAddress,
		NewRole:     node.Role,
	}

	node.Reconfigure = false

	return resp, nil
}

func (o *Orchestrator) GetClusterState(ctx context.Context, req *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if len(o.chainOrder) == 0 {
		return nil, fmt.Errorf("no nodes in cluster")
	}

	headId := o.chainOrder[0]
	tailId := o.chainOrder[len(o.chainOrder)-1]

	return &pb.GetClusterStateResponse{
		Head: &pb.NodeInfo{
			NodeId:  headId,
			Address: o.nodes[headId].Address,
		},
		Tail: &pb.NodeInfo{
			NodeId:  tailId,
			Address: o.nodes[tailId].Address,
		},
	}, nil
}

func (o *Orchestrator) monitorHealth() {
	ticker := time.NewTicker(3 * time.Second)
	for range ticker.C {
		o.mu.Lock()

		now := time.Now()
		for nodeId, lastBeat := range o.nodeHealth {
			if now.Sub(lastBeat) > 5*time.Second {
				fmt.Printf("Node %s is DEAD!\n", nodeId)
				o.handleNodeFailure(nodeId)
			}
		}

		o.mu.Unlock()
	}
}

func (o *Orchestrator) handleNodeFailure(deadNodeId string) {
	// Find position in chain
	idx := -1
	for i, id := range o.chainOrder {
		if id == deadNodeId {
			idx = i
			break
		}
	}
	if idx == -1 {
		return
	}

	// Get neighbors
	var prevId, nextId string
	if idx > 0 {
		prevId = o.chainOrder[idx-1]
	}
	if idx < len(o.chainOrder)-1 {
		nextId = o.chainOrder[idx+1]
	}

	// Remove from chain
	o.chainOrder = append(o.chainOrder[:idx], o.chainOrder[idx+1:]...)
	delete(o.nodes, deadNodeId)
	delete(o.nodeHealth, deadNodeId)
	delete(o.nodeSubs, deadNodeId)

	// Reconnect neighbors
	if prevId != "" && nextId != "" {
		o.nodes[prevId].NextAddress = o.nodes[nextId].Address
		o.nodes[prevId].Reconfigure = true
		o.nodes[nextId].PrevAddress = o.nodes[prevId].Address
		o.nodes[nextId].Reconfigure = true
	}

	// Update roles if HEAD or TAIL died
	if len(o.chainOrder) > 0 {
		o.nodes[o.chainOrder[0]].Role = "head"
		o.nodes[o.chainOrder[len(o.chainOrder)-1]].Role = "tail"
	}

	fmt.Printf("Chain reconfigured after %s failure\n", deadNodeId)
}

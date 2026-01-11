package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "razpravljalnica/proto"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Orchestrator struct {
	pb.UnimplementedOrchestratorServer

	mu       sync.RWMutex
	raft     *raft.Raft
	fsm      *FSM
	nodeId   string
	address  string
	raftDir  string
	raftBind string
}

func NewOrchestrator(nodeId, address, raftBind, raftDir string) *Orchestrator {
	return &Orchestrator{
		nodeId:   nodeId,
		address:  address,
		raftDir:  raftDir,
		raftBind: raftBind,
		fsm:      NewFSM(),
	}
}

func (o *Orchestrator) Start(bootstrap bool) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(o.nodeId)

	if err := os.MkdirAll(o.raftDir, 0755); err != nil {
		return err
	}

	addr, err := net.ResolveTCPAddr("tcp", o.raftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(o.raftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(o.raftDir, "raft-log.db"))
	if err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(o.raftDir, "raft-stable.db"))
	if err != nil {
		return err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(o.raftDir, 2, os.Stderr)
	if err != nil {
		return err
	}

	r, err := raft.NewRaft(config, o.fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}
	o.raft = r

	if bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(o.nodeId),
					Address: raft.ServerAddress(o.raftBind),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}

	go o.startGRPC()
	go o.monitorHealth()

	fmt.Printf("Orchestrator %s started (raft: %s, grpc: %s)\n", o.nodeId, o.raftBind, o.address)
	return nil
}

func (o *Orchestrator) startGRPC() {
	listener, err := net.Listen("tcp", o.address)
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterOrchestratorServer(grpcServer, o)

	fmt.Printf("gRPC listening on %s\n", o.address)
	grpcServer.Serve(listener)
}

func (o *Orchestrator) Join(nodeId, raftAddr string) error {
	if o.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	future := o.raft.AddVoter(raft.ServerID(nodeId), raft.ServerAddress(raftAddr), 0, 0)
	return future.Error()
}

func (o *Orchestrator) applyCommand(cmdType string, payload interface{}) error {
	if o.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	cmd := Command{
		Type:    cmdType,
		Payload: payloadBytes,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	future := o.raft.Apply(cmdBytes, 5*time.Second)
	return future.Error()
}

func (o *Orchestrator) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	o.fsm.mu.Lock()

	nodeId := fmt.Sprintf("node-%d", len(o.fsm.nodes)+1)
	var role, nextAddr, prevAddr, oldTailId string

	if len(o.fsm.chainOrder) == 0 {
		role = "head"
	} else {
		oldTailId = o.fsm.chainOrder[len(o.fsm.chainOrder)-1]
		oldTail := o.fsm.nodes[oldTailId]

		role = "tail"
		prevAddr = oldTail.Address
	}

	o.fsm.mu.Unlock()

	// Apply command through Raft - FSM will handle updating old tail
	err := o.applyCommand(CmdRegisterNode, RegisterNodePayload{
		NodeId:    nodeId,
		Address:   req.Address,
		Role:      role,
		Next:      nextAddr,
		Prev:      prevAddr,
		OldTailId: oldTailId,
	})
	if err != nil {
		return nil, err
	}

	fmt.Printf("Node registered: %s (%s) at %s\n", nodeId, role, req.Address)

	return &pb.RegisterNodeResponse{
		NodeId:      nodeId,
		Role:        role,
		NextAddress: nextAddr,
		PrevAddress: prevAddr,
	}, nil
}

func (o *Orchestrator) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	o.fsm.mu.Lock()
	o.fsm.nodeHealth[req.NodeId] = time.Now()
	o.fsm.nodeSubs[req.NodeId] = req.SubscriberCount

	node := o.fsm.nodes[req.NodeId]
	var resp *pb.HeartbeatResponse
	if node != nil {
		resp = &pb.HeartbeatResponse{
			Reconfigure: node.Reconfigure,
			NewNext:     node.NextAddress,
			NewPrev:     node.PrevAddress,
			NewRole:     node.Role,
		}
		node.Reconfigure = false
	} else {
		resp = &pb.HeartbeatResponse{}
	}
	o.fsm.mu.Unlock()

	return resp, nil
}

func (o *Orchestrator) GetClusterState(ctx context.Context, req *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	o.fsm.mu.RLock()
	defer o.fsm.mu.RUnlock()

	if len(o.fsm.chainOrder) == 0 {
		return nil, fmt.Errorf("no nodes in cluster")
	}

	headId := o.fsm.chainOrder[0]
	tailId := o.fsm.chainOrder[len(o.fsm.chainOrder)-1]

	return &pb.GetClusterStateResponse{
		Head: &pb.NodeInfo{
			NodeId:  headId,
			Address: o.fsm.nodes[headId].Address,
		},
		Tail: &pb.NodeInfo{
			NodeId:  tailId,
			Address: o.fsm.nodes[tailId].Address,
		},
	}, nil
}

func (o *Orchestrator) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	o.fsm.mu.Lock()
	defer o.fsm.mu.Unlock()

	if len(o.fsm.nodes) == 0 {
		return nil, fmt.Errorf("no nodes available")
	}

	var bestNodeId string
	var minSubs int32 = 1<<31 - 1

	for nodeId, subs := range o.fsm.nodeSubs {
		if subs < minSubs {
			minSubs = subs
			bestNodeId = nodeId
		}
	}

	if bestNodeId == "" {
		bestNodeId = o.fsm.chainOrder[0]
	}

	node := o.fsm.nodes[bestNodeId]
	token := generateToken()

	o.fsm.validTokens[token] = &TokenInfo{
		UserId:   req.UserId,
		TopicIds: req.TopicId,
		NodeId:   bestNodeId,
	}

	fmt.Printf("Subscription assigned to %s, token: %s\n", bestNodeId, token)

	return &pb.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node: &pb.NodeInfo{
			NodeId:  bestNodeId,
			Address: node.Address,
		},
	}, nil
}

func (o *Orchestrator) VerifyToken(ctx context.Context, req *pb.VerifyTokenRequest) (*pb.VerifyTokenResponse, error) {
	o.fsm.mu.RLock()
	defer o.fsm.mu.RUnlock()

	tokenInfo, ok := o.fsm.validTokens[req.Token]
	if !ok || tokenInfo.UserId != req.UserId {
		return &pb.VerifyTokenResponse{Valid: false}, nil
	}

	return &pb.VerifyTokenResponse{Valid: true}, nil
}

func (o *Orchestrator) monitorHealth() {
	ticker := time.NewTicker(3 * time.Second)
	for range ticker.C {
		if o.raft.State() != raft.Leader {
			continue
		}

		// Collect dead nodes first (don't hold lock while handling failure)
		o.fsm.mu.RLock()
		var deadNodes []string
		now := time.Now()
		for nodeId, lastBeat := range o.fsm.nodeHealth {
			if now.Sub(lastBeat) > 5*time.Second {
				deadNodes = append(deadNodes, nodeId)
			}
		}
		o.fsm.mu.RUnlock()

		// Handle failures without holding the lock
		for _, nodeId := range deadNodes {
			fmt.Printf("Node %s is DEAD!\n", nodeId)
			o.handleNodeFailure(nodeId)
		}
	}
}

func (o *Orchestrator) handleNodeFailure(deadNodeId string) {
	o.fsm.mu.Lock()

	// Find position and neighbors BEFORE removing
	idx := -1
	for i, id := range o.fsm.chainOrder {
		if id == deadNodeId {
			idx = i
			break
		}
	}

	if idx < 0 {
		o.fsm.mu.Unlock()
		return // Node already removed or not found
	}

	var prevId, nextId string
	var prevNode, nextNode *NodeInfo

	if idx > 0 {
		prevId = o.fsm.chainOrder[idx-1]
		prevNode = o.fsm.nodes[prevId]
	}
	if idx < len(o.fsm.chainOrder)-1 {
		nextId = o.fsm.chainOrder[idx+1]
		nextNode = o.fsm.nodes[nextId]
	}

	// Remove from chain
	o.fsm.chainOrder = append(o.fsm.chainOrder[:idx], o.fsm.chainOrder[idx+1:]...)
	delete(o.fsm.nodes, deadNodeId)
	delete(o.fsm.nodeHealth, deadNodeId)
	delete(o.fsm.nodeSubs, deadNodeId)

	// Reconnect neighbors
	if prevNode != nil && nextNode != nil {
		// Middle node died - connect prev to next
		prevNode.NextAddress = nextNode.Address
		prevNode.Reconfigure = true
		nextNode.PrevAddress = prevNode.Address
		nextNode.Reconfigure = true
	} else if prevNode != nil && nextNode == nil {
		// TAIL died - prev becomes new TAIL
		prevNode.NextAddress = ""
		prevNode.Role = "tail"
		prevNode.Reconfigure = true
	} else if prevNode == nil && nextNode != nil {
		// HEAD died - next becomes new HEAD
		nextNode.PrevAddress = ""
		nextNode.Role = "head"
		nextNode.Reconfigure = true
	}

	// Make sure roles are correct
	if len(o.fsm.chainOrder) == 1 {
		// Only one node left - it's both head and tail
		o.fsm.nodes[o.fsm.chainOrder[0]].Role = "head"
	} else if len(o.fsm.chainOrder) > 1 {
		o.fsm.nodes[o.fsm.chainOrder[0]].Role = "head"
		o.fsm.nodes[o.fsm.chainOrder[len(o.fsm.chainOrder)-1]].Role = "tail"
	}

	o.fsm.mu.Unlock()

	// Apply through Raft (without holding lock)
	err := o.applyCommand(CmdRemoveNode, RemoveNodePayload{NodeId: deadNodeId})
	if err != nil {
		fmt.Printf("Failed to apply remove command: %v\n", err)
	}

	fmt.Printf("Chain reconfigured after %s failure. Chain: %v\n", deadNodeId, o.fsm.chainOrder)
}

func generateToken() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte(time.Now().UnixNano() % 256)
	}
	return fmt.Sprintf("%x", b)
}

func (o *Orchestrator) JoinCluster(ctx context.Context, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	if o.raft.State() != raft.Leader {
		return &pb.JoinClusterResponse{
			Success: false,
			Error:   "not the leader",
		}, nil
	}

	future := o.raft.AddVoter(
		raft.ServerID(req.NodeId),
		raft.ServerAddress(req.RaftAddress),
		0, 0,
	)

	if err := future.Error(); err != nil {
		return &pb.JoinClusterResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	fmt.Printf("Node %s joined cluster at %s\n", req.NodeId, req.RaftAddress)

	return &pb.JoinClusterResponse{Success: true}, nil
}

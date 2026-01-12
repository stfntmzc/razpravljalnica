package orchestrator

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

const (
	CmdRegisterNode = "register_node"
	CmdHeartbeat    = "heartbeat"
	CmdRemoveNode   = "remove_node"
	CmdStoreToken   = "store_token"
)

type Command struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type FSM struct {
	mu          sync.RWMutex
	nodes       map[string]*NodeInfo
	chainOrder  []string
	nodeHealth  map[string]time.Time
	nodeSubs    map[string]int32
	validTokens map[string]*TokenInfo
}

func NewFSM() *FSM {
	return &FSM{
		nodes:       make(map[string]*NodeInfo),
		chainOrder:  []string{},
		nodeHealth:  make(map[string]time.Time),
		nodeSubs:    make(map[string]int32),
		validTokens: make(map[string]*TokenInfo),
	}
}

type NodeInfo struct {
	Id          string
	Address     string
	Role        string
	NextAddress string
	PrevAddress string
	Reconfigure bool
}

type TokenInfo struct {
	UserId   int64
	TopicIds []int64
	NodeId   string
}

// Vsak log je treba applyjat
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %v", err)
	}

	switch cmd.Type {
	case CmdRegisterNode:
		return f.applyRegisterNode(cmd.Payload)
	case CmdHeartbeat:
		return f.applyHeartbeat(cmd.Payload)
	case CmdRemoveNode:
		return f.applyRemoveNode(cmd.Payload)
	case CmdStoreToken:
		return f.applyStoreToken(cmd.Payload)
	}

	return fmt.Errorf("unknown command type: %s", cmd.Type)
}

type RegisterNodePayload struct {
	NodeId    string `json:"node_id"`
	Address   string `json:"address"`
	Role      string `json:"role"`
	Next      string `json:"next"`
	Prev      string `json:"prev"`
	OldTailId string `json:"old_tail_id,omitempty"`
}

func (f *FSM) applyRegisterNode(payload json.RawMessage) interface{} {
	var p RegisterNodePayload
	json.Unmarshal(payload, &p)

	f.mu.Lock()
	defer f.mu.Unlock()

	if p.OldTailId != "" {
		if oldTail, ok := f.nodes[p.OldTailId]; ok {
			oldTail.Role = "middle"
			oldTail.NextAddress = p.Address
			oldTail.Reconfigure = true
			fmt.Printf("[FSM] Old tail %s reconfigured to middle, next=%s\n", p.OldTailId, p.Address)
		}
	}

	node := &NodeInfo{
		Id:          p.NodeId,
		Address:     p.Address,
		Role:        p.Role,
		NextAddress: p.Next,
		PrevAddress: p.Prev,
	}

	f.nodes[p.NodeId] = node
	f.chainOrder = append(f.chainOrder, p.NodeId)
	f.nodeHealth[p.NodeId] = time.Now()

	fmt.Printf("[FSM] Node registered: %s (%s)\n", p.NodeId, p.Role)
	return nil
}

type HeartbeatPayload struct {
	NodeId    string `json:"node_id"`
	SubCount  int32  `json:"sub_count"`
	Timestamp int64  `json:"timestamp"`
}

func (f *FSM) applyHeartbeat(payload json.RawMessage) interface{} {
	var p HeartbeatPayload
	json.Unmarshal(payload, &p)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.nodeHealth[p.NodeId] = time.Unix(0, p.Timestamp)
	f.nodeSubs[p.NodeId] = p.SubCount

	return nil
}

type RemoveNodePayload struct {
	NodeId string `json:"node_id"`
}

func (f *FSM) applyRemoveNode(payload json.RawMessage) interface{} {
	var p RemoveNodePayload
	json.Unmarshal(payload, &p)

	f.mu.Lock()
	defer f.mu.Unlock()

	idx := -1
	for i, id := range f.chainOrder {
		if id == p.NodeId {
			idx = i
			break
		}
	}
	if idx >= 0 {
		f.chainOrder = append(f.chainOrder[:idx], f.chainOrder[idx+1:]...)
	}

	delete(f.nodes, p.NodeId)
	delete(f.nodeHealth, p.NodeId)
	delete(f.nodeSubs, p.NodeId)

	fmt.Printf("[FSM] Node removed: %s\n", p.NodeId)
	return nil
}

type TokenPayload struct {
	Token    string  `json:"token"`
	UserId   int64   `json:"user_id"`
	TopicIds []int64 `json:"topic_ids"`
	NodeId   string  `json:"node_id"`
}

func (f *FSM) applyStoreToken(payload json.RawMessage) interface{} {
	var p TokenPayload
	json.Unmarshal(payload, &p)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.validTokens[p.Token] = &TokenInfo{
		UserId:   p.UserId,
		TopicIds: p.TopicIds,
		NodeId:   p.NodeId,
	}

	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	snapshot := &FSMSnapshot{
		Nodes:       make(map[string]*NodeInfo),
		ChainOrder:  make([]string, len(f.chainOrder)),
		ValidTokens: make(map[string]*TokenInfo),
	}

	for k, v := range f.nodes {
		snapshot.Nodes[k] = v
	}
	copy(snapshot.ChainOrder, f.chainOrder)
	for k, v := range f.validTokens {
		snapshot.ValidTokens[k] = v
	}

	return snapshot, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshot FSMSnapshot
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.nodes = snapshot.Nodes
	f.chainOrder = snapshot.ChainOrder
	f.validTokens = snapshot.ValidTokens

	return nil
}

type FSMSnapshot struct {
	Nodes       map[string]*NodeInfo  `json:"nodes"`
	ChainOrder  []string              `json:"chain_order"`
	ValidTokens map[string]*TokenInfo `json:"valid_tokens"`
}

func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *FSMSnapshot) Release() {}

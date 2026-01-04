// implementacija strežnika.
// logika
// ta paket se uvaža v cmd/server/main.go za zagon strežnika

package server

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"net"
	pb "razpravljalnica/proto"
	"time"

	"slices"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// =============================================
// MESSAGEBOARD SERVER

var wg sync.WaitGroup

type MessageBoardServer struct {
	pb.UnimplementedMessageBoardServer
	id            string
	url           string
	mu            sync.RWMutex
	topics        map[int64]*pb.Topic
	nextTopicID   int64
	messages      map[int64]*pb.Message
	nextMessageID int64
	users         map[int64]*pb.User
	nextUserID    int64
	// subscribe stvari
	subscribers        map[int64][]chan *pb.MessageEvent // map vseh subscriberjev
	subscribersMu      sync.RWMutex
	subscribersPerNode map[string]int64            // kolk je subscriberjov na posamznem nodeu. sicer ga rabi samo head, ampak zaenkrat tkole
	subscriptions      map[string]subscriptionInfo // token -> informacije o neki naročnini. tudo to rabi samo head
	nextSeqNum         int64
	// replikacija
	isHead     bool
	isTail     bool
	nodeNext   *adjacentNode
	nodePrev   *adjacentNode
	nodeId     string
	orchClient pb.OrchestratorClient
}

type subscriptionInfo struct {
	userId  int64
	nodeId  string
	topicId int64
}

type adjacentNode struct {
	conn   *grpc.ClientConn
	rpc    pb.MessageBoardClient
	cancel context.CancelFunc
}

func newMessageBoardServer(url string, id string, isHead bool, isTail bool) *MessageBoardServer {
	return &MessageBoardServer{
		id:                 id,
		url:                url,
		topics:             make(map[int64]*pb.Topic, 0),
		nextTopicID:        1,
		messages:           make(map[int64]*pb.Message, 0),
		nextMessageID:      1,
		users:              make(map[int64]*pb.User, 0),
		nextUserID:         1,
		subscribers:        make(map[int64][]chan *pb.MessageEvent),
		nextSeqNum:         1,
		subscribersPerNode: make(map[string]int64),            // kolk je subscriberjov na posamznem nodeu. sicer ga rabi samo head, ampak zaenkrat tkole
		subscriptions:      make(map[string]subscriptionInfo), // token -> informacije o neki naročnini. tudo to rabi samo head
		isHead:             isHead,
		isTail:             isTail,
		nodeNext:           nil,
		nodePrev:           nil,
	}
}

func getAdjacentNode(url string) (*adjacentNode, error) {
	fmt.Printf("Connecting to node: %s\n", url)
	// cancel funkcija za ugasnt
	ctx, cancel := context.WithCancel(context.Background())

	var rpc pb.MessageBoardClient
	var conn *grpc.ClientConn

	var err error
	retry := 5
	for retry != 0 {
		// nov "client"
		conn, err = grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
		//defer conn.Close()
		if err != nil {
			cancel()
			return nil, err
		}
		rpc = pb.NewMessageBoardClient(conn)
		// test povezave
		_, err = rpc.TestConnection(ctx, &emptypb.Empty{})
		if err == nil {
			break
		}
		fmt.Printf("Failed to connect to %s, retrying...\n", url)
		retry--
		time.Sleep(time.Second * 2)
	}
	if err != nil {
		cancel()
		return nil, err
	}

	return &adjacentNode{
		conn:   conn,
		rpc:    rpc,
		cancel: cancel,
	}, nil
}

func (server *MessageBoardServer) connectToNode(url string, direction bool) error {
	node, err := getAdjacentNode(url)
	if err != nil {
		return err
	}

	if direction {
		server.nodeNext = node
		fmt.Printf("Connected to next node at %s\n", url)
	}
	if !direction {
		server.nodePrev = node
		fmt.Printf("Connected to previous node at %s\n", url)
	}

	return nil
}

// MESSAGEBOARD SERVER
// =============================================

func StartServer(url string, orchestratorAddr string) {

	orchConn, err := grpc.NewClient(orchestratorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	orchClient := pb.NewOrchestratorClient(orchConn)

	resp, err := orchClient.RegisterNode(context.Background(), &pb.RegisterNodeRequest{
		Address: url,
	})
	if err != nil {
		panic(err)
	}

	// avtomatsko dodeli role

	isHead := resp.Role == "head"
	isTail := resp.Role == "tail"

	fmt.Printf("Registered as %s (role: %s)\n", resp.NodeId, resp.Role)

	// pripravimo grpc strežnik
	grpcServer := grpc.NewServer()

	// registreramo servis (message board)
	messageBoardServer := newMessageBoardServer(url, resp.NodeId, isHead, isTail)
	messageBoardServer.orchClient = orchClient
	pb.RegisterMessageBoardServer(grpcServer, messageBoardServer)
	// odpremo port
	listener, err := net.Listen("tcp", url)
	if err != nil {
		panic(err)
	}

	// tudi avtomatsko dodeli sosede
	if resp.NextAddress != "" {
		go func() {
			messageBoardServer.connectToNode(resp.NextAddress, true)
		}()
	}
	if resp.PrevAddress != "" {
		go func() {
			messageBoardServer.connectToNode(resp.PrevAddress, false)
		}()
	}

	// zacne posiljat heartbeat za monitoring
	go messageBoardServer.heartbeatLoop()

	fmt.Printf("gRPC server listening at %v\n", url)
	// začnemo s streženjem
	if err := grpcServer.Serve(listener); err != nil {
		panic(err)
	}
}

// =============================================
// MESSAGEBOARD SERVER FUNKCIJE

func (server *MessageBoardServer) TestConnection(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (server *MessageBoardServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if server.nodeNext != nil {
		_, err := server.nodeNext.rpc.CreateTopic(ctx, req)

		if err != nil {
			return nil, err
		}
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	topic, exists := server.getTopicByName(req.Name)
	if exists {
		fmt.Printf("NEW TOPIC ATTEMPT BY [%d] %s, BUT TOPIC ALREADY EXISTS: Name=%s, Id=%d\n", req.UserId, server.users[req.UserId].Name, topic.Name, topic.Id)
		return nil, status.Errorf(codes.AlreadyExists, "topic '%s' already exists\n", req.Name)
	}
	newTopic := &pb.Topic{Id: server.nextTopicID, Name: req.Name}
	server.topics[newTopic.Id] = newTopic
	server.nextTopicID++
	fmt.Printf("NEW TOPIC CREATED BY [%d] %s: Name=%s, Id=%d\n", req.UserId, server.users[req.UserId].Name, newTopic.Name, newTopic.Id)
	return newTopic, nil
}

// pomozna
func (server *MessageBoardServer) getTopicByName(name string) (*pb.Topic, bool) {

	for _, topic := range server.topics {
		if topic.Name == name {
			return topic, true
		}
	}
	return nil, false
}

func (server *MessageBoardServer) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {

	if server.nodeNext != nil {
		_, err := server.nodeNext.rpc.CreateUser(ctx, req)

		if err != nil {
			return nil, err
		}
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	user, exists := server.getUserByName(req.Name)
	if exists {
		fmt.Printf("USER CONNECTED: Name=%s, Id=%d\n", user.Name, user.Id)
		return user, nil
	}
	newUser := &pb.User{Id: server.nextUserID, Name: req.Name}
	server.users[newUser.Id] = newUser
	server.nextUserID++
	fmt.Printf("NEW USER CONNECTED: Name=%s, Id=%d\n", newUser.Name, newUser.Id)
	return newUser, nil
}

// pomozna
func (server *MessageBoardServer) getUserByName(name string) (*pb.User, bool) {
	for _, user := range server.users {
		if user.Name == name {
			return user, true
		}
	}
	return nil, false
}

func (server *MessageBoardServer) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	if server.nodeNext != nil {
		_, err := server.nodeNext.rpc.PostMessage(ctx, req)
		if err != nil {
			return nil, err
		}
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	user, ok := server.users[req.UserId]
	if !ok {
		// user not found !!!!!! neki čudnega se dogaja
		return nil, fmt.Errorf("user with id %d not found\n", req.UserId)
	}
	// preverimo ce topic obstaja
	topic, ok := server.topics[req.TopicId]
	if !ok {
		fmt.Printf("NEW MESSAGE ATTEMPT BY [%d] %s, BUT TOPIC WITH ID %d DOES NOT EXIST\n", user.Id, user.Name, req.TopicId)
		return nil, fmt.Errorf("topic with id %d not found", req.TopicId)
	}

	// ustvari novo sporočilo
	msg := &pb.Message{
		Id:        server.nextMessageID,
		TopicId:   topic.Id,
		UserId:    user.Id,
		Text:      req.Text,
		Likes:     0,
		CreatedAt: timestamppb.Now(), // za enkrat
	}

	// shranimo sporočilo v mapo
	server.messages[msg.Id] = msg
	server.nextMessageID++

	fmt.Printf("NEW MESSAGE BY [%d] %s ON TOPIC [%d] %s: [%d] %s\n", user.Id, user.Name, topic.Id, topic.Name, msg.Id, msg.Text)

	// modified da se lahko broadcasta
	server.broadcast(msg.TopicId, pb.OpType_OP_POST, msg)
	return msg, nil
}

func (server *MessageBoardServer) UpdateMessage(ctx context.Context, req *pb.UpdateMessageRequest) (*pb.Message, error) {

	if server.nodeNext != nil {
		_, err := server.nodeNext.rpc.UpdateMessage(ctx, req)

		if err != nil {
			return nil, err
		}
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	msg := server.messages[req.MessageId]
	if msg == nil {
		return nil, fmt.Errorf("Message with id %d does not exist\n", req.MessageId)
	}
	oldText := msg.Text
	server.messages[req.MessageId].Text = req.Text
	fmt.Printf("MESSAGE [%d] '%s' UPDATED TO: %s\n", req.MessageId, oldText, server.messages[req.MessageId].Text)

	// same here
	server.broadcast(msg.TopicId, pb.OpType_OP_UPDATE, msg)
	return server.messages[req.MessageId], nil
}

func (server *MessageBoardServer) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*emptypb.Empty, error) {

	if server.nodeNext != nil {
		_, err := server.nodeNext.rpc.DeleteMessage(ctx, req)

		if err != nil {
			return nil, err
		}
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	msg := server.messages[req.MessageId]
	if msg == nil {
		fmt.Printf("DELETE MESSAGE ATTEMPT BY [%d] %s, BUT MESSAGE WITH ID %d DOES NOT EXIST\n", req.UserId, server.users[req.UserId].Name, req.MessageId)
		return &emptypb.Empty{}, fmt.Errorf("Message with id %d does not exist\n", req.MessageId)
	}
	delete(server.messages, req.MessageId)
	fmt.Printf("MESSAGE WITH ID %d DELETED\n", req.MessageId)
	// same here
	server.broadcast(msg.TopicId, pb.OpType_OP_DELETE, msg)
	return &emptypb.Empty{}, nil
}

func (server *MessageBoardServer) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {

	if server.nodeNext != nil {
		_, err := server.nodeNext.rpc.LikeMessage(ctx, req)

		if err != nil {
			return nil, err
		}
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	// zacommentano ce bi pol rabla se obvestit kdo ti je lajkal al pa kaj
	topic_id := req.TopicId
	message_id := req.MessageId
	// user_id  := req.UserId

	message, ok := server.messages[message_id]
	if !ok {
		// message ne obstaja
		fmt.Printf("CAN'T LIKE MESSAGE WITH ID %d BECAUSE IT DOESN'T EXIST\n", message_id)
		return nil, fmt.Errorf("message with id %d not found", message_id)
	}

	message.Likes += 1

	fmt.Printf("SUCCCESSFULLY LIKED A MESSAGE WITH ID %d FROM TOPIC WITH ID %d\n", message_id, topic_id)
	server.broadcast(message.TopicId, pb.OpType_OP_LIKE, message)
	return message, nil
}

func (server *MessageBoardServer) ListTopics(ctx context.Context, req *emptypb.Empty) (*pb.ListTopicsResponse, error) {

	// smo ze na tailu tak da ne abimo rekurzivno klicat nic
	// ampak se vedno mormo lockat za branje, zato uporabimo rlock
	server.mu.RLock()
	defer server.mu.RUnlock()

	topics_slice := slices.Collect(maps.Values(server.topics))

	response := &pb.ListTopicsResponse{
		Topics: topics_slice,
	}

	fmt.Printf("SUCCESSFULLY LISTED ALL TOPICS!\n")

	return response, nil
}

func (server *MessageBoardServer) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	// isto tu, smo na tailu
	server.mu.RLock()
	defer server.mu.RUnlock()

	topic_id := req.TopicId
	from_id := req.FromMessageId
	limit := req.Limit

	messages_slice := make([]*pb.Message, 0, limit)

	i := int32(0)

	for _, message := range server.messages {
		if from_id <= message.Id && topic_id == message.TopicId && i < limit {
			i += 1
			messages_slice = append(messages_slice, message)
		}
	}

	response := &pb.GetMessagesResponse{
		Messages: messages_slice,
	}

	fmt.Printf("SUCCESSFULLY GOT ALL MESSAGES\n")
	return response, nil
}

// subscribe stvari
// TODO: da je to avtomatsko
// zaenkrat
var nodeAdresses = map[string]string{
	// nodeID -> address
	"1": "localhost:9876",
	"2": "localhost:9877",
	"3": "localhost:9878",
	"4": "localhost:9879",
}

func (server *MessageBoardServer) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	if !server.isHead {
		return nil, fmt.Errorf("can't request subscription node from non-head node")
	}
	// preverimo če topic obstaja
	_, ok := server.topics[req.TopicId[0]]
	if !ok {
		return nil, fmt.Errorf("topic with id %d doesn't exist", req.TopicId[0])
	}

	nodeId, ok := server.getLeastSubscribersNodeId()
	if !ok {
		return nil, fmt.Errorf("no nodes available")
	}
	token := server.generateToken()
	res := &pb.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node: &pb.NodeInfo{
			NodeId:  nodeId,
			Address: nodeAdresses[nodeId],
		},
	}

	fmt.Printf("New subscription (unverefied): %s\n", res)
	server.mu.Lock()
	defer server.mu.Unlock()

	subInfo := server.subscriptions[token]
	subInfo.userId = req.UserId
	subInfo.topicId = req.TopicId[0]
	subInfo.nodeId = nodeId
	server.subscriptions[token] = subInfo
	server.subscribersPerNode[nodeId]++

	return res, nil
}

// pomožna
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func (server *MessageBoardServer) generateToken() string {
	b := make([]rune, 16)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// pomožna
func (server *MessageBoardServer) getLeastSubscribersNodeId() (string, bool) {
	first := true
	var minSubs int64
	var minId string
	for nodeId, count := range server.subscribersPerNode {
		if first || count < minSubs {
			minId = nodeId
			minSubs = count
			first = false
		}
	}

	if first {
		// mapa je prazna
		return "", false
	}

	return minId, true
}

func (server *MessageBoardServer) SubscribeTopic(req *pb.SubscribeTopicRequest, stream grpc.ServerStreamingServer[pb.MessageEvent]) error {

	// preverimo token
	tokenResp, err := server.orchClient.VerifyToken(context.Background(), &pb.VerifyTokenRequest{
		Token:  req.SubscribeToken,
		UserId: req.UserId,
	})
	if err != nil || !tokenResp.Valid {
		return fmt.Errorf("invalid token")
	}

	// token je ok, nadaljujemo

	ch := make(chan *pb.MessageEvent, 100)

	server.subscribersMu.Lock()
	for _, topicId := range req.TopicId {
		server.subscribers[topicId] = append(server.subscribers[topicId], ch)
	}
	server.subscribersMu.Unlock()

	defer func() {
		server.subscribersMu.Lock()
		for _, topicId := range req.TopicId {
			channels := server.subscribers[topicId]
			for i, c := range channels {
				if c == ch {
					server.subscribers[topicId] = append(channels[:i], channels[i+1:]...)
					break
				}
			}
			fmt.Printf("USER [%d] UNSUBSCRIBED FROM TOPIC [%d]\n", req.UserId, req.TopicId[0])
		}
		server.subscribersMu.Unlock()
		close(ch)
	}()

	fmt.Printf("NEW SUBSCRIBER [%d] TO TOPIC [%d]\n", req.UserId, req.TopicId[0])
	for {
		select {
		case event := <-ch:
			if err := stream.Send(event); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (server *MessageBoardServer) VerifyToken(ctx context.Context, req *pb.VerifyTokenRequest) (*pb.VerifyTokenResponse, error) {
	server.mu.RLock()
	defer server.mu.RUnlock()
	if server.isHead {
		var status bool = false
		subInfo, ok := server.subscriptions[req.Token]
		if ok && subInfo.userId == req.UserId {
			status = true
		}
		fmt.Printf("Subscription %s authorised\n", req.Token)
		return &pb.VerifyTokenResponse{
			Valid: status,
		}, nil
	}
	return server.nodePrev.rpc.VerifyToken(ctx, req)
}

func (server *MessageBoardServer) ExpireSubscription(ctx context.Context, req *pb.ExpireSubscriptionRequest) (*emptypb.Empty, error) {
	if !server.isHead {
		return nil, fmt.Errorf("Non head node")
	}
	server.mu.RLock()
	subInfo, ok := server.subscriptions[req.Token]
	server.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("Invalid token")
	}
	if subInfo.userId != req.UserId {
		return nil, fmt.Errorf("Wrong user")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	server.subscribersPerNode[subInfo.nodeId]--
	delete(server.subscriptions, req.Token)
	fmt.Printf("USER [%d] %s UNSUBSCRIBED FROM TOPIC [%d] ON NODE %s\n", req.UserId, server.users[req.UserId].Name, server.subscriptions[req.Token].topicId, subInfo.nodeId)
	return nil, nil
}

// helper funkcija
func (server *MessageBoardServer) broadcast(topicId int64, op pb.OpType, msg *pb.Message) {
	event := &pb.MessageEvent{
		SequenceNumber: server.nextSeqNum,
		Op:             op,
		Message:        msg,
		EventAt:        timestamppb.Now(),
	}
	server.nextSeqNum++

	server.subscribersMu.RLock()
	for _, ch := range server.subscribers[topicId] {
		select {
		case ch <- event:
		default:
			// poln kanal
		}
	}
	server.subscribersMu.RUnlock()
}

func (server *MessageBoardServer) heartbeatLoop() {
	ticker := time.NewTicker(1 * time.Second)

	for range ticker.C {
		// DEBUG: fmt.Println("Sending heartbeat...")

		server.subscribersMu.RLock()
		subCount := int32(0)

		for _, subs := range server.subscribers {
			subCount += int32(len(subs))
		}
		server.subscribersMu.RUnlock()

		resp, err := server.orchClient.Heartbeat(context.Background(), &pb.HeartbeatRequest{
			NodeId:          server.id,
			SubscriberCount: subCount,
		})

		if err != nil {
			fmt.Println("Heartbeat failed", err)
			continue
		}

		if resp.Reconfigure {
			server.reconfigure(resp)
		}
	}
}

func (server *MessageBoardServer) reconfigure(resp *pb.HeartbeatResponse) {
	fmt.Printf("Reconfiguring: role=%s, next=%s, prev=%s\n", resp.NewRole, resp.NewNext, resp.NewPrev)

	server.isHead = resp.NewRole == "head"
	server.isTail = resp.NewRole == "tail"

	if resp.NewNext != "" {
		go server.connectToNode(resp.NewNext, true)
	}
	if resp.NewPrev != "" {
		go server.connectToNode(resp.NewPrev, false)
	}
}

// MESSAGEBOARD SERVER FUNKCIJE
// =============================================

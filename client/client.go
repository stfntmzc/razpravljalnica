// implementacija odjemalca.
// logika
// ta paket se uvaža v cmd/client/main.go za zagon odjemalca

package client

import (
	"bufio"
	"context"
	"fmt"
	"os"
	pb "razpravljalnica/proto"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type CommandHandler func(clientState *ClientState, args []string)
type ClientState struct {
	connHead      *grpc.ClientConn
	rpcHead       pb.MessageBoardClient
	connTail      *grpc.ClientConn
	rpcTail       pb.MessageBoardClient
	user          *pb.User
	ctx           context.Context
	cancel        context.CancelFunc
	subscriptions map[int64]Subscription
	orchClient    pb.OrchestratorClient
	subConn       *grpc.ClientConn
	subCancel     context.CancelFunc
}
type Subscription struct {
	connSub   *grpc.ClientConn
	rpcSub    pb.MessageBoardClient
	cancelSub context.CancelFunc
	token     string
}

// mapa komand
var commands = map[string]CommandHandler{}

func Client(orchestratorAddr string, username string) {

	// inicializacija mape komand
	initCommandHandlers()

	// povežemo se na strežnik
	clientState, err := connectToServer(orchestratorAddr, username)
	if err != nil {
		panic(err)
	}
	defer clientState.connHead.Close()
	defer clientState.connTail.Close()

	// main loop
	scanner := bufio.NewScanner(os.Stdin)
	for {
		select {
		case <-clientState.ctx.Done():
			fmt.Println("Client exiting...")
			return
		default:
			fmt.Printf("> ")
			if !scanner.Scan() {
				break
			}
			line := scanner.Text()
			if line == "" {
				continue
			}
			//fmt.Println("ukaz:", line)
			handleInput(clientState, line)
		}
	}
}

func initCommandHandlers() {
	commands["/q"] = quitHandler
	commands["/write"] = writeHandler
	commands["/newtopic"] = newtopicHandler
	commands["/edit"] = editHandler
	commands["/del"] = delHandler
	commands["/like"] = likeHandler
	commands["/topics"] = listTopicsHandler
	commands["/messages"] = listMessagesHandler
	commands["/node"] = getSubscriptionNodeHandler
	commands["/subscribe"] = subscribtionHandler
	commands["/unsubscribe"] = unsubscribeHandler
}

func handleInput(clientState *ClientState, line string) {
	tokens := strings.Fields(line)
	if len(tokens) == 0 {
		return
	}
	commandName := tokens[0]
	args := tokens[1:]
	if cmd, ok := commands[commandName]; ok {
		cmd(clientState, args)
	} else {
		fmt.Println("Unknown command:", commandName)
	}
}

// =============================================
// COMMANDS

func writeHandler(clientState *ClientState, args []string) {
	if len(args) <= 1 {
		fmt.Println("Usage: /write <topic_id> <text>")
		return
	}

	var topicID int64
	_, err := fmt.Sscan(args[0], &topicID)
	if err != nil {
		fmt.Println("Invalid topic_id (must be a number)")
		return
	}

	text := strings.Join(args[1:], " ")
	//topicID := int64(1) // za enkrat
	// naredimo message request
	req := &pb.PostMessageRequest{
		TopicId: topicID,
		UserId:  clientState.user.Id,
		Text:    text,
	}

	message, err := clientState.rpcHead.PostMessage(clientState.ctx, req)
	if err != nil {
		fmt.Println("Error posting message:", err)
		return
	}
	fmt.Printf("Message posted on topic %d: %s\n", message.TopicId, message.Text)
}

func quitHandler(clientState *ClientState, args []string) {
	// pošlje signal v kanal
	clientState.cancel()
}

func newtopicHandler(clientState *ClientState, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /newtopic <name_of_new_topic>")
		return
	}

	name := strings.Join(args, " ")
	req := &pb.CreateTopicRequest{
		Name:   name,
		UserId: clientState.user.Id,
	}
	topic, err := clientState.rpcHead.CreateTopic(clientState.ctx, req)
	if err != nil {
		fmt.Println("Error creating topic:", err)
		return
	}
	fmt.Printf("New topic created: Name=%s, Id=%d\n", topic.Name, topic.Id)
}

func editHandler(clientState *ClientState, args []string) {
	if len(args) <= 1 {
		fmt.Println("Usage: /edit <message_id> <updated_message>")
		return
	}
	var messageId int64
	_, err := fmt.Sscan(args[0], &messageId)
	if err != nil {
		fmt.Println("Invalid message_id (must be a number)")
		return
	}
	text := strings.Join(args[1:], " ")
	req := &pb.UpdateMessageRequest{
		MessageId: messageId,
		Text:      text,
	}
	message, err := clientState.rpcHead.UpdateMessage(clientState.ctx, req)
	if err != nil {
		fmt.Println("Error updating message:", err)
		return
	}
	fmt.Printf("Message updated: Text=%s, Id=%d\n", message.Text, message.Id)
}

func delHandler(clientState *ClientState, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /del <message_id>")
		return
	}
	var messageId int64
	_, err1 := fmt.Sscan(args[0], &messageId)
	if err1 != nil {
		fmt.Println("Invalid message_id (must be a number)")
		return
	}
	req := &pb.DeleteMessageRequest{
		MessageId: messageId,
		UserId:    clientState.user.Id,
	}
	_, err2 := clientState.rpcHead.DeleteMessage(clientState.ctx, req)
	if err2 != nil {
		fmt.Println("Error deleting message:", err2)
		return
	}
	fmt.Printf("Message with id %d deleted\n", messageId)
}

func likeHandler(clientState *ClientState, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /like <message_id>")
		return
	}

	var messageId int64
	_, err := fmt.Sscan(args[0], &messageId)
	if err != nil {
		fmt.Println("Invalid message_id (must be a number)")
		return
	}

	req := &pb.LikeMessageRequest{
		MessageId: messageId,
		UserId:    clientState.user.Id,
	}

	msg, err := clientState.rpcHead.LikeMessage(clientState.ctx, req)
	if err != nil {
		fmt.Println("Error liking message:", err)
		return
	}

	fmt.Printf("Liked message %d (now has %d likes)\n", messageId, msg.Likes)
}

func listTopicsHandler(clientState *ClientState, args []string) {
	response, err := clientState.rpcTail.ListTopics(clientState.ctx, &emptypb.Empty{})
	if err != nil {
		fmt.Println("Error listing topics:", err)
		return
	}

	fmt.Println("Topics:")
	for _, topic := range response.Topics {
		fmt.Printf("  [%d] %s\n", topic.Id, topic.Name)
	}
}

func listMessagesHandler(clientState *ClientState, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /messages <topic_id> [from_id] [limit]")
		return
	}

	var topicId int64
	fmt.Sscan(args[0], &topicId)

	var fromId int64 = 0
	if len(args) > 1 {
		fmt.Sscan(args[1], &fromId)
	}

	var limit int32 = 50
	if len(args) > 2 {
		fmt.Sscan(args[2], &limit)
	}

	req := &pb.GetMessagesRequest{
		TopicId:       topicId,
		FromMessageId: fromId,
		Limit:         limit,
	}

	response, err := clientState.rpcTail.GetMessages(clientState.ctx, req)
	if err != nil {
		fmt.Println("Error getting messages:", err)
		return
	}

	fmt.Printf("Messages in topic %d:\n", topicId)
	for _, msg := range response.Messages {
		fmt.Printf("  [%d] User %d: %s (likes: %d)\n", msg.Id, msg.UserId, msg.Text, msg.Likes)
	}
}

func getSubscriptionNodeHandler(clientState *ClientState, args []string) {
	fmt.Println("Ne rabima se zaj")
}

func subscribtionHandler(clientState *ClientState, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /subscribe <topic_id> [topic_id2] ...")
		return
	}

	if clientState.subCancel != nil {
		clientState.subCancel()
	}
	if clientState.subConn != nil {
		clientState.subConn.Close()
	}

	var topicIds []int64
	for _, arg := range args {
		var id int64
		_, err := fmt.Sscan(arg, &id)
		if err != nil {
			fmt.Println("Invalid topic_id:", arg)
			return
		}
		topicIds = append(topicIds, id)
	}

	nodeResp, err := clientState.orchClient.GetSubscriptionNode(clientState.ctx, &pb.SubscriptionNodeRequest{
		UserId:  clientState.user.Id,
		TopicId: topicIds,
	})
	if err != nil {
		fmt.Println("Error getting subscription node:", err)
		return
	}

	fmt.Printf("Assigned to node %s at %s\n", nodeResp.Node.NodeId, nodeResp.Node.Address)

	subConn, err := grpc.NewClient(nodeResp.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println("Error connecting to subscription node:", err)
		return
	}
	clientState.subConn = subConn
	subClient := pb.NewMessageBoardClient(subConn)

	subCtx, subCancel := context.WithCancel(clientState.ctx)
	clientState.subCancel = subCancel

	stream, err := subClient.SubscribeTopic(subCtx, &pb.SubscribeTopicRequest{
		TopicId:        topicIds,
		UserId:         clientState.user.Id,
		SubscribeToken: nodeResp.SubscribeToken,
	})
	if err != nil {
		fmt.Println("Error subscribing:", err)
		subCancel()
		subConn.Close()
		return
	}

	fmt.Println("Subscribed to topics:", topicIds)

	go func() {
		for {
			event, err := stream.Recv()
			if err != nil {
				if subCtx.Err() == context.Canceled {
					return
				}
				fmt.Println("\nSubscription ended:", err)
				return
			}

			opName := ""
			switch event.Op {
			case pb.OpType_OP_POST:
				opName = "NEW"
			case pb.OpType_OP_LIKE:
				opName = "LIKE"
			case pb.OpType_OP_UPDATE:
				opName = "EDIT"
			case pb.OpType_OP_DELETE:
				opName = "DELETE"
			}

			fmt.Printf("\n[%s] Topic %d, Msg %d: %s (likes: %d)\n> ",
				opName, event.Message.TopicId, event.Message.Id,
				event.Message.Text, event.Message.Likes)
		}
	}()
}

func unsubscribeHandler(clientState *ClientState, args []string) {
	if clientState.subCancel == nil {
		fmt.Println("Not subscribed to anything")
		return
	}
	clientState.subCancel()
	clientState.subCancel = nil
	if clientState.subConn != nil {
		clientState.subConn.Close()
		clientState.subConn = nil
	}
	fmt.Println("Unsubscribed")
}

// COMMANDS
// =============================================

func connectToServer(orchestratorAddr string, username string) (*ClientState, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// connectas na orchestrator
	orchConn, err := grpc.NewClient(orchestratorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		return nil, err
	}
	orchClient := pb.NewOrchestratorClient(orchConn)

	// dobis cluster state
	clusterState, err := orchClient.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to get cluster state: %v", err)
	}

	fmt.Printf("Head: %s, Tail: %s\n", clusterState.Head.Address, clusterState.Tail.Address)

	// connectas na head
	connHead, err := grpc.NewClient(clusterState.Head.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		return nil, err
	}
	clientHead := pb.NewMessageBoardClient(connHead)

	// connectas na tail
	connTail, err := grpc.NewClient(clusterState.Tail.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		connHead.Close()
		cancel()
		return nil, err
	}
	clientTail := pb.NewMessageBoardClient(connTail)

	// registriras userja na head
	user, err := clientHead.CreateUser(ctx, &pb.CreateUserRequest{Name: username})
	if err != nil {
		connHead.Close()
		connTail.Close()
		cancel()
		return nil, err
	}

	fmt.Printf("Logged in as %s (id: %d)\n", user.Name, user.Id)

	return &ClientState{
		connHead:   connHead,
		rpcHead:    clientHead,
		connTail:   connTail,
		rpcTail:    clientTail,
		user:       user,
		ctx:        ctx,
		cancel:     cancel,
		orchClient: orchClient,
	}, nil
}

func testConnection(client pb.MessageBoardClient, ctx context.Context) error {
	pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
	defer pingCancel()
	_, err := client.TestConnection(pingCtx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	return nil
}

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
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CommandHandler func(clientState *ClientState, args []string)
type ClientState struct {
	ConnHead      *grpc.ClientConn
	rpcHead       pb.MessageBoardClient
	ConnTail      *grpc.ClientConn
	rpcTail       pb.MessageBoardClient
	User          *pb.User
	Ctx           context.Context
	cancel        context.CancelFunc
	Subscriptions map[int64]Subscription
	orchClient    pb.OrchestratorClient // novo
	//subConn       *grpc.ClientConn      // novo mislim da
	//subCancel     context.CancelFunc
	// za ui
	SubscriptionEventsChan chan UiSubscriptionEventItem
}
type Subscription struct {
	connSub   *grpc.ClientConn
	rpcSub    pb.MessageBoardClient
	cancelSub context.CancelFunc
	token     string
}

// za ui
type UiSubscriptionEventItem struct {
	Username  string
	UserId    int64
	OpByUser  int64
	MessageId int64
	Timestamp *timestamppb.Timestamp
	Likes     int64
	OpType    string
	Text      string
	TopicId   int64
}

// mapa komand
var commands = map[string]CommandHandler{}

func ClientUi(orchestratorAddr string, username string) (*ClientState, error) {

	// inicializacija mape komand
	initCommandHandlers()

	// povežemo se na strežnik
	clientState, err := ConnectToServer(orchestratorAddr, username)
	if err != nil {
		panic(err)
	}

	return clientState, nil
}

func Client(orchestratorAddr string, username string) {

	// inicializacija mape komand
	initCommandHandlers()

	// povežemo se na strežnik
	clientState, err := ConnectToServer(orchestratorAddr, username)
	if err != nil {
		panic(err)
	}
	defer clientState.ConnHead.Close()
	defer clientState.ConnTail.Close()
	defer UnsubscribeFromAll(clientState)
	fmt.Println("Coneccted to server")
	fmt.Printf("Username=%s, UserId=%d\n", username, clientState.User.Id)

	// main loop
	scanner := bufio.NewScanner(os.Stdin)
	for {
		select {
		case <-clientState.Ctx.Done():
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

// treba reconnectat vsakic v primeru da se karkoli spremeni
func (client *ClientState) refreshConnections() error {
	clusterState, err := client.orchClient.GetClusterState(client.ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}

	client.connHead.Close()
	client.connHead, _ = grpc.NewClient(clusterState.Head.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client.rpcHead = pb.NewMessageBoardClient(client.connHead)

	client.connTail.Close()
	client.connTail, _ = grpc.NewClient(clusterState.Tail.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client.rpcTail = pb.NewMessageBoardClient(client.connTail)

	fmt.Printf("Reconnected: HEAD=%s, TAIL=%s\n",
		clusterState.Head.Address, clusterState.Tail.Address)

	return nil
}

func writeHandler(clientState *ClientState, args []string) {
	if len(args) <= 1 {
		fmt.Println("Usage: /write <topic_id> <text>")
		return
	}

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
		return
	}

	var topicID int64
	_, err := fmt.Sscan(args[0], &topicID)
	if err != nil {
		fmt.Println("Invalid topic_id (must be a number)")
		return
	}

	text := strings.Join(args[1:], " ")

	// naredimo message request
	req := &pb.PostMessageRequest{
		TopicId: topicID,
		UserId:  clientState.User.Id,
		Text:    text,
	}

	message, err := clientState.rpcHead.PostMessage(clientState.Ctx, req)
	if err != nil {
		fmt.Println("Error posting message:", err)
		return
	}
	fmt.Printf("Message posted on topic %d: %s\n", message.TopicId, message.Text)
}

// za ui
func PostMessage(clientState *ClientState, topicId int64, text string) (*UiMessageItem, error) {

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
		return
	}

	req := &pb.PostMessageRequest{
		TopicId: topicId,
		UserId:  clientState.User.Id,
		Text:    text,
	}
	message, err := clientState.rpcHead.PostMessage(clientState.Ctx, req)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("Message posted on topic %d: %s\n", message.TopicId, message.Text)
	return &UiMessageItem{
		Username:  clientState.User.Name,
		UserId:    clientState.User.Id,
		Timestamp: message.CreatedAt,
		Likes:     0,
		Text:      []string{message.Text},
		TopicId:   message.TopicId,
	}, nil
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

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
		return
	}

	name := strings.Join(args, " ")
	req := &pb.CreateTopicRequest{
		Name:   name,
		UserId: clientState.User.Id,
	}
	topic, err := clientState.rpcHead.CreateTopic(clientState.Ctx, req)
	if err != nil {
		fmt.Println("Error creating topic:", err)
		return
	}
	fmt.Printf("New topic created: Name=%s, Id=%d\n", topic.Name, topic.Id)
}

// za ui
func CreateTopic(clientState *ClientState, name string) error {
	if name == "" {
		return fmt.Errorf("topic can't have empty name")
	}

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
		return
	}

	req := &pb.CreateTopicRequest{
		Name:   name,
		UserId: clientState.User.Id,
	}
	_, err := clientState.rpcHead.CreateTopic(clientState.Ctx, req)
	if err != nil {
		return err
	}
	return nil
	//fmt.Printf("New topic created: Name=%s, Id=%d\n", topic.Name, topic.Id)
}

func editHandler(clientState *ClientState, args []string) {
	if len(args) <= 1 {
		fmt.Println("Usage: /edit <message_id> <updated_message>")
		return
	}

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
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
	message, err := clientState.rpcHead.UpdateMessage(clientState.Ctx, req)
	if err != nil {
		fmt.Println("Error updating message:", err)
		return
	}
	fmt.Printf("Message updated: Text=%s, Id=%d\n", message.Text, message.Id)
}

// za ui
func EditMessage(clientState *ClientState, messageId int, text string) error {

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
		return
	}

	req := &pb.UpdateMessageRequest{
		MessageId: int64(messageId),
		Text:      text,
		UserId:    clientState.User.Id,
	}
	_, err := clientState.rpcHead.UpdateMessage(clientState.Ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func delHandler(clientState *ClientState, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /del <message_id>")
		return
	}

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
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
		UserId:    clientState.User.Id,
	}
	_, err2 := clientState.rpcHead.DeleteMessage(clientState.Ctx, req)
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

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
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
		UserId:    clientState.User.Id,
	}

	msg, err := clientState.rpcHead.LikeMessage(clientState.Ctx, req)
	if err != nil {
		fmt.Println("Error liking message:", err)
		return
	}

	fmt.Printf("Liked message %d (now has %d likes)\n", messageId, msg.Likes)
}

// za ui
func LikeMessage(clientState *ClientState, messageId int64, topicId int) error {
	req := &pb.LikeMessageRequest{
		MessageId: messageId,
		UserId:    clientState.User.Id,
		TopicId:   int64(topicId),
	}

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
		return
	}

	//fmt.Printf("%d", messageId)
	_, err := clientState.rpcHead.LikeMessage(clientState.Ctx, req)
	if err != nil {
		return err
	}
	return nil
}

// za ui
func DeleteMessage(clientState *ClientState, messageId int64, topicId int) error {

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
		return
	}

	req := &pb.DeleteMessageRequest{
		MessageId: messageId,
		UserId:    clientState.User.Id,
		TopicId:   int64(topicId),
	}
	//fmt.Printf("%d", messageId)
	_, err := clientState.rpcHead.DeleteMessage(clientState.Ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func listTopicsHandler(clientState *ClientState, args []string) {

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
		return
	}

	response, err := clientState.rpcTail.ListTopics(clientState.Ctx, &emptypb.Empty{})
	if err != nil {
		fmt.Println("Error listing topics:", err)
		return
	}

	fmt.Println("Topics:")
	for _, topic := range response.Topics {
		fmt.Printf("  [%d] %s\n", topic.Id, topic.Name)
	}
}

// za ui
func GetTopics(clientState *ClientState) (map[int64]string, error) {

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
		return
	}

	response, err := clientState.rpcTail.ListTopics(clientState.Ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	topics := make(map[int64]string)
	for _, topic := range response.Topics {
		topics[topic.Id] = topic.Name
	}
	return topics, nil
}

func listMessagesHandler(clientState *ClientState, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /messages <topic_id> [from_id] [limit]")
		return
	}

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
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

	response, err := clientState.rpcTail.GetMessages(clientState.Ctx, req)
	if err != nil {
		fmt.Println("Error getting messages:", err)
		return
	}

	fmt.Printf("Messages in topic %d:\n", topicId)
	for _, msg := range response.Messages {
		getUserReq := &pb.GetUserRequest{
			UserId:    msg.UserId,
			RequestBy: clientState.User.Id,
		}
		user, err := clientState.rpcTail.GetUser(clientState.Ctx, getUserReq)
		if err != nil {
			fmt.Println("Error getting username:", err)
			fmt.Printf(" [%d] ??? [%d]: %s (likes: %d)\n", msg.UserId, msg.Id, msg.Text, msg.Likes)
		}
		fmt.Printf(" [%d] %s [%d]: %s (likes: %d)\n", msg.Id, user.Name, msg.Id, msg.Text, msg.Likes)
	}
}

// za ui
type UiMessageItem struct {
	Username  string
	UserId    int64
	Id        int64
	Timestamp *timestamppb.Timestamp
	Likes     int64
	Text      []string // array stringov širine messageItemWidth
	TopicId   int64
}

func ListMessages(clientState *ClientState, topicId int64) (map[int64]UiMessageItem, error) {

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
		return
	}
	req := &pb.GetMessagesRequest{
		TopicId:       topicId,
		FromMessageId: 0,
		Limit:         -1,
	}
	messages, err := clientState.rpcTail.GetMessages(clientState.Ctx, req)
	if err != nil {
		//fmt.Println("Error getting messages:", err)
		return nil, fmt.Errorf("Error getting messages")
	}
	// naredimo map
	uiMessages := make(map[int64]UiMessageItem)
	for _, msg := range messages.Messages {
		getUserReq := &pb.GetUserRequest{
			UserId:    msg.UserId,
			RequestBy: clientState.User.Id,
		}
		user, err := clientState.rpcTail.GetUser(clientState.Ctx, getUserReq)
		if err != nil {
			continue
		}
		uiMessages[msg.Id] = UiMessageItem{
			//Username: fmt.Sprintf("user_%d", msg.UserId), // začasno
			Username:  fmt.Sprintf("%s", user.Name),
			UserId:    msg.UserId,
			Id:        msg.Id,
			Timestamp: msg.CreatedAt,
			Likes:     int64(msg.Likes),
			Text:      []string{msg.Text},
			TopicId:   msg.TopicId,
		}
	}
	return uiMessages, nil
}

func getSubscriptionNodeHandler(clientState *ClientState, args []string) {
	fmt.Println("Ne rabima")
}

func subscribtionHandler(clientState *ClientState, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /subscribe <topic_id> [topic_id2] ...")
		return
	}

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
		return
	}

	/*if clientState.subCancel != nil {
		clientState.subCancel()
	}
	if clientState.subConn != nil {
		clientState.subConn.Close()
	}*/

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
	//fmt.Println(topicIds)

	for _, topicId := range topicIds {

		// prevermo če je že subscribed na ta topic
		if _, exists := clientState.Subscriptions[topicId]; exists {
			fmt.Printf("Already subscribed to topic with id %d\n", topicId)
			continue
		}

		// tukej dobimo subscribe node
		nodeReq := &pb.SubscriptionNodeRequest{
			UserId:  clientState.User.Id,
			TopicId: []int64{topicId},
		}

		// to zdaj handla orchestrator
		nodeResp, err := clientState.orchClient.GetSubscriptionNode(clientState.Ctx, nodeReq)
		if err != nil {
			fmt.Println("Error getting subscription node:", err)
			continue
		}
		fmt.Printf("Assigned to node %s at %s for topic %d\n", nodeResp.Node.NodeId, nodeResp.Node.Address, topicId)

		// Naredimo ločen context za to subscription
		subCtx, cancel := context.WithCancel(clientState.Ctx)

		// povežemo se na subscribe node
		conn, err := grpc.NewClient(nodeResp.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("Connection to node %s unsucsessful: %s\n", nodeResp.Node.Address, err)
			continue
		}

		// rpc client
		rpc := pb.NewMessageBoardClient(conn)
		err = testConnection(rpc, subCtx)
		if err != nil {
			conn.Close()
			conn.Close()
			fmt.Printf("Connection to node %s unsucsessful: %s\n", nodeResp.Node.Address, err)
			continue
		}
		fmt.Printf("Succsessfuly connected to node %s for subscription to topic %d\n", nodeResp.Node.Address, topicId)

		// "registreramo" subscription na clientu
		clientState.Subscriptions[topicId] = Subscription{
			connSub:   conn,
			rpcSub:    rpc,
			cancelSub: cancel,
			token:     nodeResp.SubscribeToken,
		}

		stream, err := clientState.Subscriptions[topicId].rpcSub.SubscribeTopic(subCtx, &pb.SubscribeTopicRequest{
			TopicId:        []int64{topicId},
			UserId:         clientState.User.Id,
			SubscribeToken: nodeResp.SubscribeToken,
		})
		if err != nil {
			fmt.Println("Error subscribing:", err)
			clientState.Subscriptions[topicId].cancelSub()
			clientState.Subscriptions[topicId].connSub.Close()
			//subCancel()
			//subConn.Close()
			return
		}

		fmt.Println("Subscribed to topic:", topicId)

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

				getUserReq := &pb.GetUserRequest{
					UserId:    event.ExecutedById,
					RequestBy: clientState.User.Id,
				}
				user, err := clientState.rpcTail.GetUser(clientState.Ctx, getUserReq)
				if err != nil {
					fmt.Println("Error getting username:", err)
					fmt.Printf("\n[%s] [%d] ??? [%d]: %s (likes: %d)\n> ", opName, event.ExecutedById, event.Message.Id, event.Message.Text, event.Message.Likes)
				}
				fmt.Printf("\n[%s] [%d] %s [%d]: %s (likes: %d)\n> ", opName, event.ExecutedById, user.Name, event.Message.Id, event.Message.Text, event.Message.Likes)

				/*fmt.Printf("\n[%s] Topic %d, Msg %d: %s (likes: %d)\n> ",
				opName, event.Message.TopicId, event.Message.Id,
				event.Message.Text, event.Message.Likes)*/
			}
		}()
	}
}

func unsubscribeHandler(clientState *ClientState, args []string) {

	if len(args) == 0 {
		fmt.Println("Usage: /unsubscribe <topic_id> [topic_id2] ...")
		return
	}

	if refreshErr := clientState.refreshConnections(); refreshErr != nil {
		fmt.Println("Failed to reconnect:", refreshErr)
		return
	}

	//var unsubscribedCount int
	for _, arg := range args {
		var topicId int64
		_, err := fmt.Sscan(arg, &topicId)
		if err != nil {
			fmt.Println("Invalid topic_id:", arg)
			continue
		}

		_, exists := clientState.Subscriptions[topicId]
		if exists {
			clientState.Subscriptions[topicId].cancelSub()
			delete(clientState.Subscriptions, topicId)
			fmt.Printf("Unsubscribed from topic %d\n", topicId)
		} else {
			fmt.Printf("Not subscribed to topic %d\n", topicId)
		}
	}
}

func UnsubscribeFromAll(clientState *ClientState) error {
	for topicId, _ := range clientState.Subscriptions {
		err := UnsubscribeFromTopic(clientState, topicId)
		if err != nil {
			return err
		}
	}
	return nil
}

// za ui
func UnsubscribeFromTopic(clientState *ClientState, topicId int64) error {
	_, exists := clientState.Subscriptions[topicId]
	if exists {
		clientState.Subscriptions[topicId].cancelSub()
		delete(clientState.Subscriptions, topicId)
		return nil
	}
	return fmt.Errorf("Not subscribed to topic %d", topicId)
}

// za ui
func SubscribeToTopic(clientState *ClientState, topicId int64) error {

	// prevermo če je že subscribed na ta topic
	if _, exists := clientState.Subscriptions[topicId]; exists {
		return fmt.Errorf("already subscribed to topic with id %d", topicId)
	}

	// tukej dobimo subscribe node
	nodeReq := &pb.SubscriptionNodeRequest{
		UserId:  clientState.User.Id,
		TopicId: []int64{topicId},
	}

	// to zdaj handla orchestrator
	nodeResp, err := clientState.orchClient.GetSubscriptionNode(clientState.Ctx, nodeReq)
	if err != nil {
		return err
	}

	// Naredimo ločen context za to subscription
	subCtx, cancel := context.WithCancel(clientState.Ctx)

	// povežemo se na subscribe node
	conn, err := grpc.NewClient(nodeResp.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	// rpc client
	rpc := pb.NewMessageBoardClient(conn)
	err = testConnection(rpc, subCtx)
	if err != nil {
		conn.Close()
		return err
	}

	// "registreramo" subscription na clientu
	clientState.Subscriptions[topicId] = Subscription{
		connSub:   conn,
		rpcSub:    rpc,
		cancelSub: cancel,
		token:     nodeResp.SubscribeToken,
	}

	stream, err := clientState.Subscriptions[topicId].rpcSub.SubscribeTopic(subCtx, &pb.SubscribeTopicRequest{
		TopicId:        []int64{topicId},
		UserId:         clientState.User.Id,
		SubscribeToken: nodeResp.SubscribeToken,
	})
	if err != nil {
		//fmt.Println("Error subscribing:", err)
		clientState.Subscriptions[topicId].cancelSub()
		clientState.Subscriptions[topicId].connSub.Close()
		return err
	}

	//fmt.Println("Subscribed to topic:", topicId)

	go func() {
		for {
			event, err := stream.Recv()
			if err != nil {
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

			getUserReq := &pb.GetUserRequest{
				UserId:    event.ExecutedById,
				RequestBy: clientState.User.Id,
			}

			user, err := clientState.rpcHead.GetUser(clientState.Ctx, getUserReq)
			if err != nil {
				continue
			}

			uiEvent := UiSubscriptionEventItem{
				Username:  fmt.Sprintf("%s", user.Name),
				UserId:    event.Message.UserId,
				OpByUser:  event.ExecutedById,
				MessageId: event.Message.Id,
				Timestamp: event.EventAt,
				Likes:     int64(event.Message.Likes),
				OpType:    opName,
				Text:      event.Message.Text,
				TopicId:   event.Message.TopicId,
			}

			select {
			case clientState.SubscriptionEventsChan <- uiEvent:
				// poslano UI-ju
			case <-clientState.Ctx.Done():
				return
			}
		}
	}()

	return nil
}

// COMMANDS
// =============================================

// poveze se preko orchestratorja, ne preko serverja
// na zacetku na login screenu je treba podati samo address od orchestratorja (ponavadi localhost:8000) in username
func ConnectToServer(orchestratorAddr string, username string) (*ClientState, error) {
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
		ConnHead:               connHead,
		rpcHead:                clientHead,
		ConnTail:               connTail,
		rpcTail:                clientTail,
		User:                   user,
		Ctx:                    ctx,
		cancel:                 cancel,
		Subscriptions:          make(map[int64]Subscription),
		orchClient:             orchClient,
		SubscriptionEventsChan: make(chan UiSubscriptionEventItem, 100),
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

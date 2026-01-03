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
	connHead      *grpc.ClientConn
	rpcHead       pb.MessageBoardClient
	connTail      *grpc.ClientConn
	rpcTail       pb.MessageBoardClient
	User          *pb.User
	Ctx           context.Context
	cancel        context.CancelFunc
	Subscriptions map[int64]Subscription // topic id -> subscription
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

func ClientUi(username string, urlHead string, urlTail string) (*ClientState, error) {

	// inicializacija mape komand
	initCommandHandlers()

	// povežemo se na strežnik
	clientState, err := connectToServer(username, urlHead, urlTail)
	if err != nil {
		panic(err)
	}

	return clientState, nil
}

func Client(username string, urlHead string, urlTail string) {

	// inicializacija mape komand
	initCommandHandlers()

	// povežemo se na strežnik
	clientState, err := connectToServer(username, urlHead, urlTail)
	if err != nil {
		panic(err)
	}
	defer clientState.connHead.Close()
	defer clientState.connTail.Close()
	fmt.Printf("Connected to servers: head=%s, tail=%s\n", urlHead, urlTail)
	fmt.Printf("Username=%s, UserId=%d\n", username, clientState.User.Id)

	//return clientState, nil

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
	//fmt.Printf("%d", messageId)
	_, err := clientState.rpcHead.LikeMessage(clientState.Ctx, req)
	if err != nil {
		return err
	}
	return nil
}

// za ui
func DeleteMessage(clientState *ClientState, messageId int64, topicId int) error {
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
		fmt.Printf("  [%d] User %d: %s (likes: %d)\n", msg.Id, msg.UserId, msg.Text, msg.Likes)
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
	fmt.Println("Ne rabima se zaj")
}

func subscribtionHandler(clientState *ClientState, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /subscribe <topic_id> [topic_id2] ...")
		return
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

	// Za vsak topik naredimo ločeno subscription
	for _, topicId := range topicIds {
		// Če je že subscribed, preskočimo
		if _, exists := clientState.Subscriptions[topicId]; exists {
			fmt.Printf("Already subscribed to topic with id %d\n", topicId)
			continue
		}

		// Naredimo ločen context za to subscription
		subCtx, cancel := context.WithCancel(clientState.Ctx)

		// tukej dobimo subscribe node
		nodeReq := &pb.SubscriptionNodeRequest{
			UserId:  clientState.User.Id,
			TopicId: []int64{topicId},
		}
		// dobimo subscription node
		subNodeResponce, err := clientState.rpcHead.GetSubscriptionNode(subCtx, nodeReq)
		if err != nil {
			fmt.Printf("Getting subscription node %s unsucsessful: %s", subNodeResponce.Node.Address, err)
			fmt.Println()
			continue
		}
		// za vsak slučaj, če kaj faila, da pošljemo expire request
		expireSubReq := &pb.ExpireSubscriptionRequest{
			Token:  subNodeResponce.SubscribeToken,
			UserId: clientState.User.Id,
			NodeId: subNodeResponce.Node.NodeId,
		}
		// povežemo se na subscribe node
		conn, err := grpc.NewClient(subNodeResponce.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("Connection to node %s unsucsessful: %s\n", subNodeResponce.Node.Address, err)
			// iz heada zbrišemo token
			clientState.rpcHead.ExpireSubscription(subCtx, expireSubReq)
			continue
		}
		rpc := pb.NewMessageBoardClient(conn)
		err = testConnection(rpc, subCtx)
		if err != nil {
			conn.Close()
			conn.Close()
			fmt.Printf("Connection to node %s unsucsessful: %s\n", subNodeResponce.Node.Address, err)
			// iz heada zbrišemo token
			clientState.rpcHead.ExpireSubscription(subCtx, expireSubReq)
			continue
		}
		fmt.Printf("Succsessfuly connected to node %s for subscription to topic %d\n", subNodeResponce.Node.Address, topicId)

		// "registreramo" subscription na clientu
		clientState.Subscriptions[topicId] = Subscription{
			connSub:   conn,
			rpcSub:    rpc,
			cancelSub: cancel,
			token:     subNodeResponce.SubscribeToken,
		}

		// zahtevamo subscription za topic
		topicReq := &pb.SubscribeTopicRequest{
			TopicId:        []int64{topicId},
			UserId:         clientState.User.Id,
			SubscribeToken: subNodeResponce.SubscribeToken,
		}

		stream, err := clientState.Subscriptions[topicId].rpcSub.SubscribeTopic(subCtx, topicReq)
		if err != nil {
			fmt.Printf("Error subscribing to topic %d: %s\n", topicId, err)
			clientState.Subscriptions[topicId].cancelSub()
			delete(clientState.Subscriptions, topicId)
			continue
		}

		fmt.Printf("Subscribed to topic: %d\n", topicId)

		// go rutina za vsak topic posebej
		go func(tId int64, s pb.MessageBoard_SubscribeTopicClient) {
			for {
				event, err := s.Recv()
				if err != nil {
					fmt.Printf("\nSubscription to topic %d ended: ", tId)
					fmt.Println(err)
					fmt.Printf("> ")
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
		}(topicId, stream)
	}
}

func unsubscribeHandler(clientState *ClientState, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /unsubscribe <topic_id> [topic_id2] ...")
		return
	}

	var unsubscribedCount int
	for _, arg := range args {
		var topicId int64
		_, err := fmt.Sscan(arg, &topicId)
		if err != nil {
			fmt.Println("Invalid topic_id:", arg)
			continue
		}

		subscription, exists := clientState.Subscriptions[topicId]
		if exists {
			req := &pb.ExpireSubscriptionRequest{
				Token:  clientState.Subscriptions[topicId].token,
				UserId: clientState.User.Id,
			}
			_, err := clientState.rpcHead.ExpireSubscription(clientState.Ctx, req)
			if err != nil {
				fmt.Printf("Error unsubscribing from topic %d: %s\n", topicId, err)
				continue
			}
			subscription.cancelSub()
			delete(clientState.Subscriptions, topicId)
			fmt.Printf("Unsubscribed from topic %d\n", topicId)
			unsubscribedCount++
		} else {
			fmt.Printf("Not subscribed to topic %d\n", topicId)
		}
	}

	if unsubscribedCount == 0 {
		fmt.Println("Not subscribed to any of the specified topics")
	}
}

func UnsubscribeFromAll(clientState *ClientState) error {
	for topicId, subscription := range clientState.Subscriptions {
		req := &pb.ExpireSubscriptionRequest{
			Token:  clientState.Subscriptions[topicId].token,
			UserId: clientState.User.Id,
		}
		_, err := clientState.rpcHead.ExpireSubscription(clientState.Ctx, req)
		if err != nil {
			return fmt.Errorf("Error unsubscribing from topic %d: %s", topicId, err)
		}
		subscription.cancelSub()
		delete(clientState.Subscriptions, topicId)
	}
	return nil
}

// za ui
func UnsubscribeFromTopic(clientState *ClientState, topicId int64) error {
	subscription, exists := clientState.Subscriptions[topicId]
	if exists {
		req := &pb.ExpireSubscriptionRequest{
			Token:  clientState.Subscriptions[topicId].token,
			UserId: clientState.User.Id,
		}
		_, err := clientState.rpcHead.ExpireSubscription(clientState.Ctx, req)
		if err != nil {
			//fmt.Printf("Error unsubscribing from topic %d: %s\n", topicId, err)
			return fmt.Errorf("Error unsubscribing from topic %d: %s", topicId, err)
		}
		subscription.cancelSub()
		delete(clientState.Subscriptions, topicId)
		//fmt.Printf("Unsubscribed from topic %d\n", topicId)
		return nil
	}
	//fmt.Printf("Not subscribed to topic %d\n", topicId)
	return fmt.Errorf("Not subscribed to topic %d", topicId)
}

// za ui
func SubscribeToTopic(clientState *ClientState, topicId int64) error {
	_, exists := clientState.Subscriptions[int64(topicId)]
	if exists {
		// smo že subscribed na ta topic
		return fmt.Errorf("already subscribed to topic with id %d", topicId)
	}

	// tukaj se mora nekaj dogajati z gorutinami, ali pa moram pošiljati v nek kanal, iz katerega bo ui bral

	// Naredimo ločen context za to subscription
	subCtx, cancel := context.WithCancel(clientState.Ctx)

	// tukej dobimo subscribe node
	nodeReq := &pb.SubscriptionNodeRequest{
		UserId:  clientState.User.Id,
		TopicId: []int64{topicId},
	}
	// dobimo subscription node
	subNodeResponce, err := clientState.rpcHead.GetSubscriptionNode(subCtx, nodeReq)
	if err != nil {
		return fmt.Errorf("Getting subscription node %s unsucsessful: %s", subNodeResponce.Node.Address, err)
	}
	// za vsak slučaj, če kaj faila, da pošljemo expire request
	expireSubReq := &pb.ExpireSubscriptionRequest{
		Token:  subNodeResponce.SubscribeToken,
		UserId: clientState.User.Id,
		NodeId: subNodeResponce.Node.NodeId,
	}
	// povežemo se na subscribe node
	conn, err := grpc.NewClient(subNodeResponce.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		// iz heada zbrišemo token
		clientState.rpcHead.ExpireSubscription(subCtx, expireSubReq)
		return fmt.Errorf("Connection to node %s unsucsessful: %s\n", subNodeResponce.Node.Address, err)
	}
	rpc := pb.NewMessageBoardClient(conn)
	err = testConnection(rpc, subCtx)
	if err != nil {
		conn.Close()
		// iz heada zbrišemo token
		clientState.rpcHead.ExpireSubscription(subCtx, expireSubReq)
		return fmt.Errorf("Connection to node %s unsucsessful: %s\n", subNodeResponce.Node.Address, err)
	}
	//fmt.Printf("Succsessfuly connected to node %s for subscription to topic %d\n", subNodeResponce.Node.Address, topicId)

	// "registreramo" subscription na clientu
	clientState.Subscriptions[topicId] = Subscription{
		connSub:   conn,
		rpcSub:    rpc,
		cancelSub: cancel,
		token:     subNodeResponce.SubscribeToken,
	}

	// zahtevamo subscription za topic
	topicReq := &pb.SubscribeTopicRequest{
		TopicId:        []int64{topicId},
		UserId:         clientState.User.Id,
		SubscribeToken: subNodeResponce.SubscribeToken,
	}

	stream, err := clientState.Subscriptions[topicId].rpcSub.SubscribeTopic(subCtx, topicReq)
	if err != nil {
		clientState.Subscriptions[topicId].cancelSub()
		delete(clientState.Subscriptions, topicId)
		return fmt.Errorf("Error subscribing to topic %d: %s\n", topicId, err)
	}

	//fmt.Printf("Subscribed to topic: %d\n", topicId)

	// gorutina
	go func(tId int64, s pb.MessageBoard_SubscribeTopicClient) {
		for {
			event, err := s.Recv()
			if err != nil {
				//fmt.Printf("\nSubscription to topic %d ended: ", tId)
				//fmt.Println(err)
				//fmt.Printf("> ")
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

			/*fmt.Printf("\n[%s] Topic %d, Msg %d: %s (likes: %d)\n> ",
			opName, event.Message.TopicId, event.Message.Id,
			event.Message.Text, event.Message.Likes)*/

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
	}(topicId, stream)

	return nil
}

// COMMANDS
// =============================================

func connectToServer(username string, urlHead string, urlTail string) (*ClientState, error) {

	// konteks, funkcija za ugasnt
	ctx, cancel := context.WithCancel(context.Background())
	// povezave
	connHead, err := grpc.NewClient(urlHead, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		return nil, err
	}
	connTail, err := grpc.NewClient(urlTail, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		return nil, err
	}
	// client, uporabnik
	clientHead := pb.NewMessageBoardClient(connHead)
	clientTail := pb.NewMessageBoardClient(connTail)
	// testiramo povezave
	err = testConnection(clientHead, ctx)
	if err != nil {
		connHead.Close()
		connTail.Close()
		cancel()
		return nil, err
	}
	//fmt.Printf("Succsessfuly connected to head: %s\n", urlHead)
	err = testConnection(clientTail, ctx)
	if err != nil {
		connHead.Close()
		connTail.Close()
		cancel()
		return nil, err
	}
	//fmt.Printf("Succsessfuly connected to tail: %s\n", urlTail)
	// registreramo clienta samo na headu
	user, err := clientHead.CreateUser(ctx, &pb.CreateUserRequest{Name: username})
	if err != nil {
		connHead.Close()
		connTail.Close()
		cancel()
		return nil, err
	}

	return &ClientState{
		connHead:               connHead,
		rpcHead:                clientHead,
		connTail:               connTail,
		rpcTail:                clientTail,
		User:                   user,
		Ctx:                    ctx,
		cancel:                 cancel,
		Subscriptions:          make(map[int64]Subscription),
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

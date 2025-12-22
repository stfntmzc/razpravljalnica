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
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CommandHandler func(clientState *ClientState, args []string)
type ClientState struct {
	conn   *grpc.ClientConn
	rpc    pb.MessageBoardClient
	user   *pb.User
	ctx    context.Context
	cancel context.CancelFunc
}

// mapa komand
var commands = map[string]CommandHandler{}
var wg sync.WaitGroup

func Client(url string, username string) {

	// inicializacija mape komand
	initCommandHandlers()

	// povežemo se na strežnik
	clientState, err := connectToServer(url, username)
	if err != nil {
		panic(err)
	}
	defer clientState.conn.Close()

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
}

func handleInput(clientState *ClientState, line string) {
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

	message, err := clientState.rpc.PostMessage(clientState.ctx, req)
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
	topic, err := clientState.rpc.CreateTopic(clientState.ctx, req)
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
	message, err := clientState.rpc.UpdateMessage(clientState.ctx, req)
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
	_, err2 := clientState.rpc.DeleteMessage(clientState.ctx, req)
	if err2 != nil {
		fmt.Println("Error deleting message:", err2)
		return
	}
	fmt.Printf("Message with id %d deleted\n", messageId)
}

func likeHandler(clientState *ClientState, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /like <post_id>")
		return
	}
	if len(args) > 2 {
		fmt.Println("Too many arguments")
		return
	}
	
	var messsageId int64
	_, err1 = fmt.Sscan(args[0], &messageId)
	if err1 != nil {
		fmt.Println("Invalid message_id (must be a number)")
		return
	}

	req := &pb.LikeMessageRequest{
		TopicId: 0 // se za implementirat po zelji
		MessageId: messageId
		UserId: 0 // se za impplementirat po zelji
	}

	_, err2 = clientState.rpc.LikeMessage(clientState.ctx, req)
	if err2 != nil {
		fmt.Println("Error liking message:", err2)
		return
	}
	
	fmt.Printf("Successfully liked a message with id: %d!", messageId)
}

func listTopicsHandler(clientState *ClientState, args []string) {
	fmt.Println("Prelen da bi implementiral rn")
}
func listMessagesHandler(clientState *ClientState, args []string) {
	fmt.Println("Prelen da bi implementiral rn")
}
func getSubscriptionNodeHandler(clientState *ClientState, args []string) {
	fmt.Println("Prelen da bi implementiral rn")
}
func subscribtionHandler(clientState *ClientState, args []string) {
	fmt.Println("Prelen da bi implementiral rn")
}

// COMMANDS
// =============================================

func connectToServer(url string, username string) (*ClientState, error) {

	// tle mislm da je treba še narest da se connecta na head in na tail

	// konteks, funkcija za ugasnt
	ctx, cancel := context.WithCancel(context.Background())
	// connection
	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		return nil, err
	}
	// client, uporabnik
	client := pb.NewMessageBoardClient(conn)
	user, err := client.CreateUser(ctx, &pb.CreateUserRequest{Name: username})
	if err != nil {
		conn.Close()
		cancel()
		return nil, err
	}

	return &ClientState{conn: conn, rpc: client, user: user, ctx: ctx, cancel: cancel}, nil
}

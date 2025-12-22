// implementacija strežnika.
// logika
// ta paket se uvaža v cmd/server/main.go za zagon strežnika

package server

import (
	"context"
	"fmt"
	"net"
	pb "razpravljalnica/proto"
	"maps"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// =============================================
// MESSAGEBOARD SERVER

type MessageBoardServer struct {
	pb.UnimplementedMessageBoardServer
	topics        map[int64]*pb.Topic
	nextTopicID   int64
	messages      map[int64]*pb.Message
	nextMessageID int64
	users         map[int64]*pb.User
	nextUserID    int64
}

func NewMessageBoardServer() *MessageBoardServer {
	return &MessageBoardServer{
		topics:        make(map[int64]*pb.Topic, 0),
		nextTopicID:   1,
		messages:      make(map[int64]*pb.Message, 0),
		nextMessageID: 1,
		users:         make(map[int64]*pb.User, 0),
		nextUserID:    1,
	}
}

// MESSAGEBOARD SERVER
// =============================================

func StartServer(url string) {
	// pripravimo grpc strežnik
	grpcServer := grpc.NewServer()

	// rekistreramo servis (message board)
	messageBoardServer := NewMessageBoardServer()
	pb.RegisterMessageBoardServer(grpcServer, messageBoardServer)

	// odpremo port
	listener, err := net.Listen("tcp", url)
	if err != nil {
		panic(err)
	}
	fmt.Printf("gRPC server listening at %v\n", url)
	// začnemo s streženjem
	if err := grpcServer.Serve(listener); err != nil {
		panic(err)
	}
}

// =============================================
// MESSAGEBOARD SERVER FUNKCIJE

func (server *MessageBoardServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	//fmt.Println("CreateTopic not implemented")
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
	// preverimo če user obstaja
	user, ok := server.users[req.UserId]
	if !ok {
		// user not found !!!!!! neki čudnega se dogaja
		return nil, fmt.Errorf("user with id %d not found", req.UserId)
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
		CreatedAt: nil, // za enkrat
	}

	// shranimo sporočilo v mapo
	server.messages[msg.Id] = msg
	server.nextMessageID++

	fmt.Printf("NEW MESSAGE BY [%d] %s ON TOPIC [%d] %s: [%d] %s\n", user.Id, user.Name, topic.Id, topic.Name, msg.Id, msg.Text)
	return msg, nil
}

func (server *MessageBoardServer) UpdateMessage(ctx context.Context, req *pb.UpdateMessageRequest) (*pb.Message, error) {
	fmt.Println("UpdateMessage not implemented")
	return nil, nil
}

func (server *MessageBoardServer) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	fmt.Println("DeleteMessage not implemented")
	// flase => ni se deletal (recimo message ne obstaja itd)
	// true => message zbrisan
	return &emptypb.Empty{}, nil
}

func (server *MessageBoardServer) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {

	topic_id := req.TopicId
	message_id := req.MessageId
	user_id  := req.UserId

	message, ok := server.messages[message_id]
	if !ok {
		// message ne obstaja
		fmt.Printf("CAN'T LIKE MESSAGE WITH ID %d BECAUSE IT DOESN'T EXIST", message_id)
		return nil, fmt.Errorf("message with id %d not found", message_id)
	}

	message.Likes += 1

	return message, nil
}

func (server *MessageBoardServer) ListTopics(ctx context.Context, req *emptypb.Empty) (*pb.ListTopicsResponse, error) {

	topics_slice := slices.Collect(maps.Values(server.topics))
	
	response := &pb.ListTopicsResponse {
		Topics: topics_slice,
	}


	return response, nil
}

func (server *MessageBoardServer) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	topic_id := req.TopicId
	from_id := req.FromMessageId
	limit := req.Limit

	messages_slice := make([]*pb.Message, 0, limit)

	i := 0

	for _, message := range server.messages {
		if from_id <= message.Id && topic_id == message.TopicId && i < limit {
			i += 1
			messages_slice = append(messages_slice, message)
		}
	}

	response := &pb.GetMessagesResponse {
		Messages: messages_slice
	}


	return response, nil
}

// subscribe stvari
func (server *MessageBoardServer) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	fmt.Println("GetSubscriptionNode not implemented")
	return nil, nil
}

func (server *MessageBoardServer) SubscribeTopic(req *pb.SubscribeTopicRequest, stream grpc.ServerStreamingServer[pb.MessageEvent]) error {
	// context je ze v stream objektu
	fmt.Println("SubscribeTopic not implemented")
	return nil
}

// MESSAGEBOARD SERVER FUNKCIJE
// =============================================

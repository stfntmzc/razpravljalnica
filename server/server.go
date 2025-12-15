// implementacija strežnika.
// logika
// ta paket se uvaža v cmd/server/main.go za zagon strežnika

package server

import (
	"context"
	"fmt"
	"net"
	pb "razpravljalnica/proto"

	"google.golang.org/grpc"
)

type MessageBoardServer struct {
	pb.UnimplementedMessageBoardServer
	messages      map[int64]*pb.Message
	nextMessageID int64
	users         map[int64]*pb.User
	nextUserID    int64
}

func NewMessageBoardServer() *MessageBoardServer {
	return &MessageBoardServer{
		messages:      make(map[int64]*pb.Message, 0),
		nextMessageID: 1,
		users:         make(map[int64]*pb.User, 0),
		nextUserID:    1,
	}
}

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

func (server *MessageBoardServer) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	user, exists := server.getUserByName(req.Name)
	if exists {
		fmt.Printf("USER JOINED: Name=%s, Id=%d\n", user.Name, user.Id)
		return user, nil
	} else {
		newUser := &pb.User{Id: server.nextUserID, Name: req.Name}
		server.users[newUser.Id] = newUser
		server.nextUserID++
		fmt.Printf("NEW USER: Name=%s, Id=%d\n", newUser.Name, newUser.Id)
		return newUser, nil
	}
}

func (server *MessageBoardServer) getUserByName(name string) (*pb.User, bool) {
	for _, user := range server.users {
		if user.Name == name {
			return user, true
		}
	}
	return nil, false
}

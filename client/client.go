// implementacija odjemalca.
// logika
// ta paket se uvaža v cmd/client/main.go za zagon odjemalca

package client

import (
	"context"
	"fmt"
	pb "razpravljalnica/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func StartClient(url string, username string) {
	fmt.Printf("gRPC client connecting to %v as user %s\n", url, username)
	// vspostavljanje povezave
	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// ustvarimo grpc clienta
	client := pb.NewMessageBoardClient(conn)

	// naredimo uporabnika
	user, err := client.CreateUser(context.Background(), &pb.CreateUserRequest{Name: username})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Connected to %v as user %s\n", url, user.Name)
}

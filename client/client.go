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

type CommandHandler func(ctx context.Context, cancel context.CancelFunc, args []string)
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

	// kontekst
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// povežemo se na strežnik
	conn, client, user, err := connectToServer(url, username)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	// main loop
	scanner := bufio.NewScanner(os.Stdin)
	for {
		select {
		case <-ctx.Done():
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
			handleInput(ctx, cancel, line)
		}

	}
}

func initCommandHandlers() {
	commands["/q"] = quitHandler
	commands["/write"] = writeHandler
}

func handleInput(ctx context.Context, cancel context.CancelFunc, line string) {
	// tokenize
	tokens := strings.Fields(line)
	if len(tokens) == 0 {
		return
	}
	commandName := tokens[0]
	args := tokens[1:]
	if cmd, ok := commands[commandName]; ok {
		cmd(ctx, cancel, args)
	} else {
		fmt.Println("Unknown command:", commandName)
	}
}

// start: commands =====================

func writeHandler(ctx context.Context, cancel context.CancelFunc, args []string) {
	//fmt.Println("write; argumenti", args)

}

func quitHandler(ctx context.Context, cancel context.CancelFunc, args []string) {
	cancel()
}

// end: commands =======================

func connectToServer(url string, username string) (*ClientState, error) {
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

	/*fmt.Printf("gRPC client connecting to %v as user %s\n", url, username)
	// vspostavljanje povezave
	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		return nil, err
	}
	//defer conn.Close()
	// ustvarimo grpc clienta
	client := pb.NewMessageBoardClient(conn)
	// naredimo uporabnika
	user, err := client.CreateUser(context.Background(), &pb.CreateUserRequest{Name: username})
	if err != nil {
		conn.Close() // zapremo, ker ne bomo vrnili
		return nil, err
	}
	fmt.Printf("Connected to %v as user %s\n", url, user.Name)
	return conn, client, user, nil*/
}

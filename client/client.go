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
}

func handleInput(clientState *ClientState, line string) {
	// tokenize
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

// start: commands =====================

func writeHandler(clientState *ClientState, args []string) {
	//fmt.Println("write; argumenti", args)
	//message := pb.NewMessage
}

func quitHandler(clientState *ClientState, args []string) {
	clientState.cancel()
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

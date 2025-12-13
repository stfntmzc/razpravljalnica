// zagonska točka za odjemalca Razpravljalnice
// zažene se z: go run cmd/client/main.go

package main

import (
	"flag"
	"fmt"
	"razpravljalnica/client"
)

func main() {
	userPtr := flag.String("u", "anon", "user")
	serverPtr := flag.String("s", "localhost", "server")
	portPtr := flag.Int("p", 9876, "port number")
	flag.Parse()

	url := fmt.Sprintf("%s:%v", *serverPtr, *portPtr)
	client.StartClient(url, *userPtr)
}

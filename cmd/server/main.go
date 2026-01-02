// zagonska točka za strežnik
// zažene se z: go run cmd/server/main.go

package main

import (
	"flag"
	"fmt"
	"razpravljalnica/server"
)

func main() {
	port := flag.Int("p", 9001, "server port")
	orchAddr := flag.String("orch", "localhost:8000", "orchestrator address")
	flag.Parse()

	url := fmt.Sprintf("localhost:%d", *port)
	server.StartServer(url, *orchAddr)
}

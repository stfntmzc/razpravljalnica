// zagonska točka za strežnik
// zažene se z: go run cmd/server/main.go

package main

import (
	"flag"
	"fmt"
	"razpravljalnica/server"
)

func main() {
	// preberemo argumente iz ukazne vrstice
	pPtr := flag.Int("p", 9876, "port number")
	flag.Parse()

	url := fmt.Sprintf("localhost:%v", *pPtr)
	server.StartServer(url)
}

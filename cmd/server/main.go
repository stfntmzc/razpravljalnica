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
	isHeadPtr := flag.Bool("h", false, "is head")
	isTailPtr := flag.Bool("t", false, "is tail")
	flag.Parse()

	// zaenkrat
	url := fmt.Sprintf("localhost:%v", *pPtr)
	urlNext := fmt.Sprintf("localhost:%v", *pPtr+1)
	urlPrev := fmt.Sprintf("localhost:%v", *pPtr-1)
	server.StartServer(url, urlNext, urlPrev, *isHeadPtr, *isTailPtr)
}

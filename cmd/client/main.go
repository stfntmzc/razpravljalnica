// zagonska točka za odjemalca Razpravljalnice
// zažene se z: go run cmd/client/main.go

package main

import (
	"flag"
	"fmt"
	"os"
	"razpravljalnica/client"
)

func main() {
	userPtr := flag.String("u", "", "user")
	serverPtr := flag.String("s", "localhost", "server")
	portPtrHead := flag.Int("h", 9876, "port number")
	portPtrTail := flag.Int("t", 9877, "port number")
	flag.Parse()

	if *userPtr == "" {
		fmt.Println("Uporaba uporabniškega imena je obvezna! Uporabite -u <uporabniško_ime>")
		os.Exit(1)
	}

	urlHead := fmt.Sprintf("%s:%v", *serverPtr, *portPtrHead)
	urlTail := fmt.Sprintf("%s:%v", *serverPtr, *portPtrTail)
	client.Client(urlHead, urlTail, *userPtr)
}

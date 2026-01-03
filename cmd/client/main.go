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
	portPtrTail := flag.Int("t", 9879, "port number")
	uiPtr := flag.Bool("g", false, "run ui instad of cli")
	flag.Parse()

	if *uiPtr {
		RunUI()
		return
	}

	if *userPtr == "" {
		fmt.Println("Uporaba uporabniškega imena je obvezna! Uporabite -u <uporabniško_ime> [-s server_url] [-h head_port] [-t head_port]")
		os.Exit(1)
	}

	urlHead := fmt.Sprintf("%s:%v", *serverPtr, *portPtrHead)
	urlTail := fmt.Sprintf("%s:%v", *serverPtr, *portPtrTail)
	client.Client(*userPtr, urlHead, urlTail)

	return
}

/*
package main

func main() {
	RunUI()
}
*/

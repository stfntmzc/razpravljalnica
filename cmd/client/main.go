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
	portPtr := flag.Int("p", 9876, "port number")
	flag.Parse()

	if *userPtr == "" {
		fmt.Println("Uporaba uporabniškega imena je obvezna! Uporabite -u <uporabniško_ime>")
		os.Exit(1)
	}

	url := fmt.Sprintf("%s:%v", *serverPtr, *portPtr)
	client.Client(url, *userPtr)
}

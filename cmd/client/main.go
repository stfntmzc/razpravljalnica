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
	userPtr := flag.String("u", "", "username")
	orchPtr := flag.String("orch", "localhost:8000", "orchestrator address")
	uiPtr := flag.Bool("tui", false, "use tui")
	flag.Parse()

	if *uiPtr {
		RunUI()
		return
	}

	if *userPtr == "" {
		fmt.Println("Username required! Use -u <username>")
		os.Exit(1)
	}

	client.Client(*orchPtr, *userPtr)
}

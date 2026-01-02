package main

import (
	"flag"
	"fmt"
	"razpravljalnica/orchestrator"
)

func main() {
	port := flag.Int("p", 8000, "orchestrator port")
	flag.Parse()

	addr := fmt.Sprintf("localhost:%d", *port)

	orch := orchestrator.NewOrchestrator()
	orch.Start(addr)
}

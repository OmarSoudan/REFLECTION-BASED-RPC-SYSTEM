package main

import (
	"fmt"
	"log"
	"time"

	"lab1/rpc"
)

type pingServer struct{}

func (*pingServer) Ping(id int) (string, error) { return fmt.Sprintf("Pong%d", id), nil }

func main() {
	server, err := rpc.NewServer(&pingServer{})
	if err != nil {
		log.Fatal(err)
	}
	if err := server.Start("127.0.0.1:0"); err != nil {
		log.Fatal(err)
	}
	defer server.Stop()

	client, err := rpc.NewClient(server.Address())
	if err != nil {
		log.Fatal(err)
	}
	for id := 1; id <= 4; id++ {
		var result string
		if err := client.Call("Ping", &result, id); err != nil {
			log.Fatal(err)
		}
		fmt.Println(result)
	}
	time.Sleep(10 * time.Millisecond)
}

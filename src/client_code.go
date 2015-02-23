package main

import (
	"fmt"
	"log"
	"net/rpc/jsonrpc"
	"os"
)

func main() {
	/* Input arg
	{"jsonrpc":"2.0","method":"lookup","params":["keyA","relationA"],"id":10}
	*/
	args_bytes := []byte(os.Args[1])

	service := "127.0.0.1:8010"
	client, err := jsonrpc.Dial("tcp", service)
	if err != nil {
		log.Fatal("Dialing Error: ", err)
	}

	var response []byte
	err = client.Call("DICT3.ServiceRequest", args_bytes, &response)
	fmt.Println("Response received!!!")
	if err != nil {
		log.Fatal("Lookup error", err)
	}
	fmt.Println("Response: ", string(response))
	//	write_to_console := os.Stdout
	//	fmt.Fprintf(write_to_console, "Response to console")
}

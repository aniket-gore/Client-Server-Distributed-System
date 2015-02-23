package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
)

type Response struct {
	Fields map[string]interface{}
}

type Request struct {
	Fields map[string]interface{}
}

type DICT3 struct {
	key, relation, value string
}

func lookup(request Request) Response {
	fmt.Println("Unmarshalled request: ", request)
	return Response{}
}

func (dict3 *DICT3) ServiceRequest(args []byte, reply *[]byte) error {
	fmt.Println("Reached server!!")
	request := Request{}
	response := Response{}

	err := json.Unmarshal(args, &request.Fields)
	checkBadRequestError(err)

	switch request.Fields["method"] {
	case "lookup":
		response = lookup(request)
	default:
		response
	}

	response.Fields = make(map[string]interface{})
	response.Fields["response"] = "Successful!"
	fmt.Println("Marshalled response: ", response)
	*reply, _ = json.Marshal(&response)
	fmt.Println("String reply:", string(*reply))
	return nil
}

func main() {
	type_dict3 := new(DICT3)
	rpc.Register(type_dict3)

	tcpAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8010")
	checkTCPError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkTCPError(err)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		jsonrpc.ServeConn(conn)
	}
}

// Check TCP connection error
func checkTCPError(err error) {
	if err != nil {
		fmt.Println("Error: ", err.Error())
		os.Exit(1)
	}
}

// Check bad request error
func checkBadRequestError(err error) {
	if err != nil {
		fmt.Println("Error: ", err.Error())
		os.Exit(1)
	}
}

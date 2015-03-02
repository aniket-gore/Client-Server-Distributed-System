package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"unicode"
)

// Request message structure
type Request struct {
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
	Id     int           `json:"id"`
}

// Response message structure
type Response struct {
	Result []interface{} `json:"result"`
	Id     int           `json:"id"`
	Error  interface{}   `json:"error"`
}

// Map to store server configuration
type config_type map[string]interface{}

func main() {
	// Read server configuration file
	var config config_type
	file, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		log.Fatal("Cannot open config file: ", err)
	}
	err = json.Unmarshal(file, &config)
	if err != nil {
		log.Fatal("Cannot read config file:", err)
	}

	// Set server connection parameters
	service := config["ipAddress"].(string) + ":" + config["port"].(string)

	// Set-up client-server connection
	client, err := jsonrpc.Dial("tcp", service)
	if err != nil {
		log.Fatal("Dialing Error: ", err)
	}

	response_channel := make(chan *rpc.Call, 100)
	var rpc_call *rpc.Call
	request_msg := Request{}
	var count_of_requests int

	scanner := bufio.NewScanner(os.Stdin)
	// Read the input file and make asynchronous requests
	for scanner.Scan() {
		args_bytes := []byte(scanner.Text())
		err = json.Unmarshal(args_bytes, &request_msg)
		// Ignore the bad requests and proceed with next requests
		if err != nil {
			fmt.Println("Error reading request JSON object.")
			continue
		}

		// Convert first character of method name to uppercase
		method_name := request_msg.Method
		method_name = string(unicode.ToUpper(rune(method_name[0]))) + method_name[1:]

		response_msg := Response{}
		count_of_requests += 1

		// Make asynchronous request to the server
		rpc_call = client.Go(config["serverID"].(string)+"."+method_name, request_msg, &response_msg, response_channel)
	}

	// Asynchronously receive all the responses from the server
	for i := 0; i < count_of_requests; i++ {
		rpc_call = <-response_channel
		if rpc_call.Error != nil {
		}
		// Print the response as a JSON object
		marshaled_response, _ := json.Marshal(rpc_call.Reply)
		// Display on Standard Output only if response not empty
		if len(marshaled_response) > 35 {
			fmt.Println(string(marshaled_response))
		}
	}
}

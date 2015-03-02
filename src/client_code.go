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

// Map to store server configuration
type config_type map[string]interface{}

// Map to store command request
type request_type map[string]interface{}

// Response msg
type response_msg_type map[string]interface{}

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

	var response []byte
	var request request_type
	response_channel := make(chan *rpc.Call, 10)
	var rpc_call *rpc.Call

	var count_of_requests int
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		// Make a series of asynchronous requests to the server
		//	for _, arg := range os.Args[2:] {
		// Read method name from request JSON object
		//		args_bytes := []byte(arg)
		args_bytes := []byte(scanner.Text())
		//	fmt.Println("Reqest: ", count_of_requests, " : ", scanner.Text())
		request = request_type{}
		err = json.Unmarshal(args_bytes, &request)
		// Ignore the bad requests and proceed with next requests
		if err != nil {
			//	fmt.Println("Error reading request JSON object.")
			continue
		}

		count_of_requests += 1
		// Convert first character of method name to uppercase
		method_name := request["method"].(string)
		method_name = string(unicode.ToUpper(rune(method_name[0]))) + method_name[1:]

		// Make asynchronous request to the server
		response = nil
		rpc_call = client.Go(config["serverID"].(string)+"."+method_name, args_bytes, &response, response_channel)
		//	}

		// Receive the responses from the server
		response_msg := response_msg_type{}
		//	var marshaled_response []byte
		//	for i := 0; i < len(os.Args[2:]); i++ {
		//	for i := 0; i < count_of_requests; i++ {
		rpc_call = <-response_channel
		if rpc_call.Error != nil {
		}
		response_msg = response_msg_type{}
		//		marshaled_reponse = []byte
		err = json.Unmarshal(*(rpc_call.Reply).(*[]byte), &response_msg)

		// Print the response as a JSON object
		marshaled_response, _ := json.Marshal(response_msg)
		fmt.Println(string(marshaled_response))
	}

}

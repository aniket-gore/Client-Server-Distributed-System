package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc/jsonrpc"
	"os"
)

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

	// Make server request
	var response []byte
	args_bytes := []byte(os.Args[2])
	err = client.Call("DICT3.ServiceRequest", args_bytes, &response)
	if err != nil {
	}
	write_to_console := os.Stdout
	fmt.Fprintf(write_to_console, string(response))
	fmt.Fprintf(write_to_console, "\n")
}

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
)

// structure for response object
type Response struct {
	Fields map[string]interface{}
}

// structure for request object
type Request struct {
	Fields map[string]interface{}
}

// structure for persistence storage
type DICT3 struct {
	Triplet  map[string]map[string]interface{}
	listener net.Listener
}

type config_type map[string]interface{}

var storageContainerPath interface{}

/**
Method: insert()
Description: This method inserts the request Triplet into persistent storage if not already present
*/
func (dict3 *DICT3) insert(request Request) Response {
	var key, relation string
	var value interface{}
	// Identify the key,relation,value from request object
	switch interface_type := request.Fields["params"].(type) {
	case []interface{}:
		for k, v := range interface_type {
			if k == 0 {
				key = v.(string)
			} else if k == 1 {
				relation = v.(string)
			} else if k == 2 {
				value = v
			}
		}
	}
	// Update the persistent storage with new triplet
	inner, ok := dict3.Triplet[key]
	if !ok {
		inner = make(map[string]interface{})
		dict3.Triplet[key] = inner
	}
	dict3.Triplet[key][relation] = value

	// Prepare the response object
	response := Response{}
	response.Fields = make(map[string]interface{})
	response.Fields["result"] = true
	response.Fields["error"] = -1
	response.Fields["id"] = request.Fields["id"]
	return response
}

/**
Method: lookup()
Description: This method performs lookup for requested key,relation and returns value if present
*/
func (dict3 *DICT3) lookup(request Request) Response {
	var key, relation string
	// Identify key,relation from request object
	switch interface_type := request.Fields["params"].(type) {
	case []interface{}:
		for k, v := range interface_type {
			if k == 0 {
				key = v.(string)
			}
			if k == 1 {
				relation = v.(string)
			}
		}
	}
	// Look-up and prepare response object
	response := Response{}
	response.Fields = make(map[string]interface{})
	if val, ok := dict3.Triplet[key][relation]; ok {
		response.Fields["result"] = val
		response.Fields["error"] = -1
	} else {
		response.Fields["result"] = nil
		response.Fields["error"] = 1
	}
	response.Fields["id"] = request.Fields["id"]
	return response
}

/**
Method: delete()
Description: This method deletes the triple identified by key,relation
*/
func (dict3 *DICT3) delete(request Request) {
	var key, relation string
	// Identify key,relation from request object
	switch interface_type := request.Fields["params"].(type) {
	case []interface{}:
		for k, v := range interface_type {
			if k == 0 {
				key = v.(string)
			}
			if k == 1 {
				relation = v.(string)
			}
		}
	}
	// Look-up and delete triple
	_, ok := dict3.Triplet[key][relation]
	if ok {
		delete(dict3.Triplet[key], relation)
		if !(len(dict3.Triplet[key]) > 1) {
			delete(dict3.Triplet, key)
		}
	}
}

/**
Method: shutdown()
Description: This method dumps the DICT3 map into a file then closes the listener and exits the program
*/
func (dict3 *DICT3) shutdown(request Request) {
	dict3.dumpToPersistentStorage()
	dict3.listener.Close()
	os.Exit(1)
}

/**
Method: ServiceRequest()
Description: Accepts requests from the client and calls appropriate method handler
*/
func (dict3 *DICT3) ServiceRequest(args []byte, reply *[]byte) error {
	// Unmarshal data and check for errors
	request := Request{}
	err := json.Unmarshal(args, &request.Fields)
	checkBadRequestError(err)

	response := Response{}
	response.Fields = make(map[string]interface{})

	// Call appropriate method handler
	switch request.Fields["method"] {
	case "lookup":
		response = dict3.lookup(request)
	case "insert":
		response = dict3.insert(request)
	case "insertOrUpdate":
		response = dict3.insert(request)
		response = Response{}
	case "delete":
		dict3.delete(request)
	case "shutdown":
		dict3.shutdown(request)
	}
	// Marshal the response
	*reply, _ = json.Marshal(&response)
	return nil
}

/**
Method: dumpToPersistentStorage()
Description: This method dumps the DICT3 data to a file before closing the connection
*/
func (dict3 *DICT3) dumpToPersistentStorage() {
	data_bytes, _ := json.Marshal(dict3.Triplet)

	filepath := storageContainerPath.(map[string]interface{})["file"].(string)
	ioutil.WriteFile(filepath, data_bytes, 0644)
}

/**
Method: fetchFromPersistentStorage()
Description: This method retrieves the the DICT3 data from the file
*/
func (dict3 *DICT3) fetchFromPersistentStorage(filemap interface{}) {
	filepath := filemap.(map[string]interface{})["file"].(string)
	file_content, err := ioutil.ReadFile(filepath)
	if err == nil {
		err = json.Unmarshal(file_content, &dict3.Triplet)
	}
}

func main() {
	type_dict3 := new(DICT3)
	type_dict3.Triplet = make(map[string]map[string]interface{})
	rpc.Register(type_dict3)

	// Read configuration file
	var config config_type
	file, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		log.Fatal("Cannot open config file: ", err)
	}
	err = json.Unmarshal(file, &config)
	if err != nil {
		log.Fatal("Cannot read config file:", err)
	}

	// Set connection parameters
	tcpAddr, err := net.ResolveTCPAddr("tcp", config["ipAddress"].(string)+":"+config["port"].(string))
	checkTCPError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	type_dict3.listener = listener
	checkTCPError(err)
	// Retrieve the DICT3 persistent storage
	type_dict3.fetchFromPersistentStorage(config["persistentStorageContainer"])
	storageContainerPath = config["persistentStorageContainer"]

	for {
		conn, err := listener.Accept()
		if err != nil {
			// ignore any errors by client side, don't shutdown server yet
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

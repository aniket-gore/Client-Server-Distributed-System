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

// Response object
type Response map[string]interface{}

// structure for request object
type Request struct {
	Fields map[string]interface{}
}

// structure for persistence storage
type DICT3 struct {
	Triplet  map[string]map[string]interface{}
	listener net.Listener
}

//Map to store server configuration
type config_type map[string]interface{}

var (
	storageContainerPath interface{}
)

/**
Method: Lookup()
Description: This method performs lookup for requested key,relation and returns value if present
*/
func (dict3 *DICT3) Lookup(args []byte, reply *[]byte) error {
	// Unmarshal data and check for errors
	request := Request{}
	err := json.Unmarshal(args, &request.Fields)
	checkBadRequestError(err)

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
	response := make(map[string]interface{})
	if val, ok := dict3.Triplet[key][relation]; ok {
		response["result"] = val
		response["error"] = nil
	} else {
		response["result"] = nil
		response["error"] = 1
	}
	response["id"] = request.Fields["id"]

	// Marshal the response
	*reply, _ = json.Marshal(&response)
	return nil
}

/**
Method: Insert()
Description: This method inserts the request Triplet into persistent storage if not already present
*/
func (dict3 *DICT3) Insert(args []byte, reply *[]byte) error {
	// Unmarshal data and check for errors
	request := Request{}
	err := json.Unmarshal(args, &request.Fields)
	checkBadRequestError(err)

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

	// Prepare the response object
	response := make(map[string]interface{})

	// Check if the key,relation are already present
	var need_to_insert bool
	inner, ok := dict3.Triplet[key]
	// If key not present, perform Insert
	if !ok {
		inner = make(map[string]interface{})
		dict3.Triplet[key] = inner
		dict3.Triplet[key][relation] = value
		need_to_insert = true
		// If key present, check if relation is present
	} else {
		_, ok := dict3.Triplet[key][relation]
		// If relation not present, perform Insert
		if !ok {
			dict3.Triplet[key][relation] = value
			need_to_insert = true
			// If relation is also present, do nothing
		} else {
			need_to_insert = false
		}
	}

	if need_to_insert {
		response["result"] = true
	} else {
		response["result"] = false
	}
	response["error"] = nil
	response["id"] = request.Fields["id"]

	// Marshal the response
	*reply, _ = json.Marshal(&response)
	return nil
}

/**
Method: InsertOrUpdate()
Description: This method inserts the request Triplet into persistent storage if not already present or updates it if present
*/
func (dict3 *DICT3) InsertOrUpdate(args []byte, reply *[]byte) error {
	// Unmarshal data and check for errors
	request := Request{}
	err := json.Unmarshal(args, &request.Fields)
	checkBadRequestError(err)

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
	response := make(map[string]interface{})
	// Marshal the response
	*reply, _ = json.Marshal(&response)
	return nil
}

/**
Method: Delete()
Description: This method deletes the triple identified by key,relation
*/
func (dict3 *DICT3) Delete(args []byte, reply *[]byte) error {
	// Unmarshal data and check for errors
	request := Request{}
	err := json.Unmarshal(args, &request.Fields)
	checkBadRequestError(err)

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

	response := make(map[string]interface{})
	// Marshal the response
	*reply, _ = json.Marshal(&response)
	return nil
}

/**
Method: ListKeys()
Description: This method lists all the keys in DICT3
*/
func (dict3 *DICT3) ListKeys(args []byte, reply *[]byte) error {
	// Unmarshal data and check for errors
	request := Request{}
	err := json.Unmarshal(args, &request.Fields)
	checkBadRequestError(err)

	// Get all unique keys from DICT3
	unique_keys := make([]string, 0, len(dict3.Triplet))
	for key := range dict3.Triplet {
		unique_keys = append(unique_keys, key)
	}

	// Prepare response object with unique keys
	response := make(map[string]interface{})
	response["result"] = unique_keys
	response["id"] = request.Fields["id"]
	response["error"] = nil

	// Marshal the response
	*reply, _ = json.Marshal(&response)
	return nil
}

/**
Method: ListIDs()
Description: This method lists all key,relation pairs in DICT3
*/
func (dict3 *DICT3) ListIDs(args []byte, reply *[]byte) error {
	// Unmarshal data and check for errors
	request := Request{}
	err := json.Unmarshal(args, &request.Fields)
	checkBadRequestError(err)

	// Get all unique key,relation pairs from DICT3
	unique_pairs := make([][]string, 0, len(dict3.Triplet))
	for key, relation_map := range dict3.Triplet {
		for relation := range relation_map {
			pair := make([]string, 0, 2)
			pair = append(pair, key)
			pair = append(pair, relation)
			unique_pairs = append(unique_pairs, pair)
		}
	}

	// Prepare response object with unique keys
	response := make(map[string]interface{})
	response["result"] = unique_pairs
	response["id"] = request.Fields["id"]
	response["error"] = nil

	// Marshal the response
	*reply, _ = json.Marshal(&response)
	return nil
}

/**
Method: Shutdown()
Description: This method dumps the DICT3 map into a file then closes the listener and exits the program
*/
func (dict3 *DICT3) Shutdown(args []byte, reply *[]byte) error {
	// Unmarshal data and check for errors
	request := Request{}
	err := json.Unmarshal(args, &request.Fields)
	checkBadRequestError(err)

	// Dump to peristent storage and close the listener
	dict3.dumpToPersistentStorage()
	dict3.listener.Close()
	os.Exit(1)

	response := make(map[string]interface{})
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

	// Register the server name
	rpc.RegisterName(config["serverID"].(string), type_dict3)

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

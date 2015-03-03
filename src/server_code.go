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

// Request message structure
type Request struct {
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
	Id     int           `json:"id"`
}

// Respose message structure
type Response struct {
	Result []interface{} `json:"result"`
	Id     int           `json:"id"`
	Error  interface{}   `json:"error"`
}

// Structure used when persistent storage is read/written from a file
type DICT3 struct {
	Triplet  map[string]map[string]interface{}
	listener net.Listener
}

//Map to store server configuration
type config_type map[string]interface{}

// Path to persistent storage file
var (
	storageContainerPath interface{}
)

/**
Method: Lookup()
Description: This method performs lookup for requested key,relation and returns value if present
*/
func (dict3 *DICT3) Lookup(request_msg Request, response_msg *Response) error {
	// Get the key,relation from request JSON object
	key := request_msg.Params[0].(string)
	relation := request_msg.Params[1].(string)

	response_msg.Result = make([]interface{}, 3)
	// Lookup for requested key,relation
	if val, ok := dict3.Triplet[key][relation]; ok {
		response_msg.Result[2] = val
		response_msg.Error = nil
	} else {
		response_msg.Result[2] = nil
		response_msg.Error = 1
	}
	response_msg.Result[0] = key
	response_msg.Result[1] = relation
	response_msg.Id = request_msg.Id
	return nil
}

/**
Method: Insert()
Description: This method inserts the request Triplet into persistent storage if not already present
*/
func (dict3 *DICT3) Insert(request_msg Request, response_msg *Response) error {
	// Get the key,relation,value from request JSON object
	key := request_msg.Params[0].(string)
	relation := request_msg.Params[1].(string)
	value := request_msg.Params[2]

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

	// Build response JSON object
	response_msg.Result = make([]interface{}, 1)
	if need_to_insert {
		response_msg.Result[0] = true
	} else {
		response_msg.Result[0] = false
	}
	response_msg.Error = nil
	response_msg.Id = request_msg.Id
	return nil
}

/**
Method: InsertOrUpdate())
Description: This method inserts the request Triplet into persistent storage if not already present or updates it if present
*/
func (dict3 *DICT3) InsertOrUpdate(request_msg Request, response_msg *Response) error {
	// Get the key,relation,value from request JSON object
	key := request_msg.Params[0].(string)
	relation := request_msg.Params[1].(string)
	value := request_msg.Params[2]

	// Insert/Update the persistent storage with new triplet
	inner, ok := dict3.Triplet[key]
	if !ok {
		inner = make(map[string]interface{})
		dict3.Triplet[key] = inner
	}
	dict3.Triplet[key][relation] = value
	return nil
}

/**
Method: Delete()
Description: This method deletes the triple identified by key,relation
*/
func (dict3 *DICT3) Delete(request_msg Request, response_msg *Response) error {
	// Get the key,relation from request JSON object
	key := request_msg.Params[0].(string)
	relation := request_msg.Params[1].(string)

	// Delete the triple if present
	_, ok := dict3.Triplet[key][relation]
	if ok {
		delete(dict3.Triplet[key], relation)
		if !(len(dict3.Triplet[key]) > 1) {
			delete(dict3.Triplet, key)
		}
	}
	return nil
}

/**
Method: ListKeys()
Description: This method lists all the keys in DICT3
*/
func (dict3 *DICT3) ListKeys(request_msg Request, response_msg *Response) error {
	// Get all unique keys from DICT3
	var index int
	response_msg.Result = make([]interface{}, len(dict3.Triplet))
	for key := range dict3.Triplet {
		response_msg.Result[index] = key
		index += 1
	}

	response_msg.Id = request_msg.Id
	response_msg.Error = nil
	return nil
}

/**
Method: ListIDs()
Description: This method lists all key,relation pairs in DICT3
*/
func (dict3 *DICT3) ListIDs(request_msg Request, response_msg *Response) error {
	// Get all unique key,relation pairs from DICT3
	var index int
	response_msg.Result = make([]interface{}, len(dict3.Triplet))
	for key, relation_map := range dict3.Triplet {
		pair := make([]string, 0, 2)
		for relation := range relation_map {
			pair = append(pair, key)
			pair = append(pair, relation)
		}
		response_msg.Result[index] = pair
		index += 1
	}

	response_msg.Id = request_msg.Id
	response_msg.Error = nil
	return nil
}

/**
Method: Shutdown()
Description: This method dumps the DICT3 map into a file then closes the listener
*/
func (dict3 *DICT3) Shutdown(request_msg Request, response_msg *Response) error {
	// Dump to peristent storage and close the listener
	dict3.dumpToPersistentStorage()
	dict3.listener.Close()
	os.Exit(1)
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
	// Initialize the DICT3 type
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
	fmt.Println("Server Started!")

	// Retrieve the DICT3 persistent storage
	type_dict3.fetchFromPersistentStorage(config["persistentStorageContainer"])
	storageContainerPath = config["persistentStorageContainer"]

	fmt.Println("Listening to requests...")
	// Indefinitely listen and serve the requests
	for {
		conn, err := listener.Accept()
		if err != nil {
			// ignore any errors by client side, don't shutdown server yet
			continue
		}
		go jsonrpc.ServeConn(conn)
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

package main

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	successorListSize = 3
)

// Node : A node for the chord system
type Node struct {
	port        int
	bucket      map[string]string
	successor   string
	predecessor string
	address     string
	identifier  *big.Int
}

// Nothing : an empty struct used for readability
type Nothing struct{}

// KeyValuePair : a struct containing a key and a value
type KeyValuePair struct {
	Key   string
	Value string
}

func main() {
	loop()
}

func loop() {
	node := new(Node)
	node.bucket = make(map[string]string)
	node.port = 3410
	node.address = getLocalAddress() + ":" + strconv.Itoa(node.port)
	node.identifier = hashString(node.address)

	go node.stabilize()

	scanner := bufio.NewScanner(os.Stdin)
Loop:
	for scanner.Scan() {
		command := strings.Fields(scanner.Text())

		if len(command) > 0 {
			switch command[0] {
			case "port":
				i, err := strconv.Atoi(command[1])
				if err == nil {
					node.port = i
					node.address = getLocalAddress() + ":" + strconv.Itoa(node.port)
					node.identifier = hashString(node.address)

					fmt.Println("Port has been set to", node.port)
				}

			case "quit":
				fmt.Println("Quitting!")
				break Loop
			case "help":
				printHelp()
			case "ping":
				ping()
			case "put":
				if len(command) == 3 {
					put(command[1], command[2], node)
				} else {
					fmt.Println("Usage: put <key> <value>")
				}
			case "get":
				if len(command) == 2 {
					get(command[1])
				} else {
					fmt.Println("Usage: get <key>")
				}
			case "delete":
				if len(command) == 2 {
					deleteKeyValuePair(command[1])
				}
			case "dump":
				node.dump()
			case "create":
				go node.create()
			case "join":
				if len(command) == 2 {
					node.join(command[1])
				}
			}
		}
	}
}

// Ping : Reply with 'pong!'
func (node *Node) Ping(junk Nothing, reply *string) error {
	fmt.Println("I've been pinged!")
	*reply = "pong!"
	return nil
}

// Put : Create or update a key/value pair in the bucket
func (node *Node) Put(input *KeyValuePair, junk *Nothing) error {
	fmt.Println(input.Key, input.Value)
	keyHash := hashString(input.Key)
	successorIdentifier := hashString(node.successor)
	if keyHash.Cmp(successorIdentifier) <= 0 {
		node.bucket[input.Key] = input.Value
	} else {
		call(node.successor, "Node.Put", input, junk)
	}
	return nil
}

// Get : Retrieve a key/value pair from the bucket
func (node *Node) Get(key string, value *string) error {
	if val, exists := node.bucket[key]; exists {
		*value = val
	}
	return nil
}

// Delete : Remove a key/value pair from the bucket
func (node *Node) Delete(key string, junk *Nothing) error {
	if _, exists := node.bucket[key]; exists {
		delete(node.bucket, key)
	}
	return nil
}

func printHelp() {
	fmt.Println("\nUsage:")
	fmt.Println("\tport <number>\tset port to <number>")
	fmt.Println("\tquit\t\tclose the service")
	fmt.Printf("\thelp\t\tprint this message\n\n")
}

func hashString(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

func getLocalAddress() string {
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to deg addresses for network interface")
			}

			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}

	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}

	return localaddress
}

func (node *Node) create() {
	fmt.Println("Creating a new node instance!")

	rpc.Register(node)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+strconv.Itoa(node.port))
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	if err := http.Serve(l, nil); err != nil {
		log.Fatalf("http.Serve %v", err)
	}
}

func call(address string, method string, request interface{}, reply interface{}) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatalf("rpc.DialHTTP: %v", err)
	}

	if err = client.Call(method, request, reply); err != nil {
		log.Fatalf("Error in call %v: %v", method, err)
	}

	client.Close()
	return nil
}

func ping() {
	fmt.Println("Pinging localhost:3410")
	var junk Nothing
	var returnValue string
	call("localhost:3410", "Node.Ping", junk, &returnValue)
	fmt.Println(returnValue)
}

func (node *Node) join(address string) {
	node.successor = address
	go node.create()
	node.dump()
}

func (node *Node) dump() {
	fmt.Println("Table Data:")
	if node != nil {
		for key, value := range node.bucket {
			fmt.Println("Key: ", key, "Value: ", value)
		}
	}

	if node.successor != "" {
		fmt.Println("Successor: ", node.successor)
	} else {
		fmt.Println("Successor: <nil>")
	}
	fmt.Println("Self: ", node.address)
	if node.predecessor != "" {
		fmt.Println("Predecessor: ", node.predecessor)
	} else {
		fmt.Println("Predecessor: <nil>")
	}

	fmt.Println("\nIdentifier: ", node.identifier)
}

func (node *Node) stabilize() {
	var junk Nothing
	var successorPredecessorAddress string
	for {
		if node.successor != "" {
			call(node.successor, "Node.GetPredecessor", junk, &successorPredecessorAddress)
			if successorPredecessorAddress != "" && node.identifier.Cmp(hashString(successorPredecessorAddress)) != 0 {
				node.successor = successorPredecessorAddress
			}
			call(node.successor, "Node.Notify", node.address, &junk)
		}
		if node.successor == "" && node.predecessor != "" {
			node.successor = node.predecessor
		}
		time.Sleep(time.Second)
	}
}

// TODO: move these two functions to places that make sense

// GetPredecessor : return's node.predecessor
func (node *Node) GetPredecessor(junk Nothing, predecessorAddress *string) error {
	*predecessorAddress = node.predecessor
	return nil
}

// Notify : Inform a node about a change in it's predecessor
func (node *Node) Notify(predecessorAddress string, junk *Nothing) error {
	if node.predecessor == "" || hashString(node.predecessor).Cmp(hashString(predecessorAddress)) <= 0 && predecessorAddress != "" {
		node.predecessor = predecessorAddress
	} else if predecessorAddress != "" {
		node.successor = predecessorAddress
	}
	return nil
}

func put(key, value string, node *Node) {
	fmt.Printf("Putting key/value pair with key: %s and value %s into the server\n", key, value)

	inputs := KeyValuePair{key, value}
	var junk *Nothing

	call(node.successor, "Node.Put", inputs, &junk)
}

func get(key string) {
	fmt.Printf("Retrieving %s from the server\n", key)

	var value string
	call("localhost:3410", "Node.Get", key, &value)

	fmt.Println(value)
}

func deleteKeyValuePair(key string) {
	fmt.Printf("Deleting %s from the server\n", key)

	var junk Nothing
	call("localhost:3410", "Node.Delete", key, &junk)
}

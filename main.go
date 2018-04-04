package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

const (
	successorListSize = 3
)

type Node struct {
	port   int
	bucket map[string]string
}

type Nothing struct{}

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
					put(command[1], command[2])
				} else {
					fmt.Println("Usage: put <key> <value>")
				}
			case "get":
				if len(command) == 2 {
					get(command[1])
				} else {
					fmt.Println("Usage: get <key>")
				}
			case "create":
				create(node)
			}
		}
	}
}

func (n *Node) Ping(junk Nothing, reply *string) error {
	fmt.Println("I've been pinged!")
	*reply = "pong!"
	return nil
}

func (n *Node) Put(input *KeyValuePair, junk *Nothing) error {
	fmt.Println(input.Key, input.Value)
	n.bucket[input.Key] = input.Value
	return nil
}

func (n *Node) Get(key string, value *string) error {
	*value = n.bucket[key]
	return nil
}

func printHelp() {
	fmt.Println("\nUsage:")
	fmt.Println("\tport <number>\tset port to <number>")
	fmt.Println("\tquit\t\tclose the service")
	fmt.Println("\thelp\t\tprint this message\n")
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

func create(node *Node) {
	fmt.Println("Creating a new node instance!")

	rpc.Register(node)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "localhost:"+strconv.Itoa(node.port))
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
	if err := call("localhost:3410", "Node.Ping", junk, &returnValue); err != nil {
		log.Fatalf("Error in 'call' function: %v", err)
	}
	fmt.Println(returnValue)
}

func put(key, value string) {
	fmt.Printf("Putting key/value pair with key: %s and value %s into the server\n", key, value)

	inputs := KeyValuePair{key, value}
	var junk *Nothing

	if err := call("localhost:3410", "Node.Put", inputs, &junk); err != nil {
		log.Fatalf("Error in 'call' function in the 'put' command: %v", err)
	}
}

func get(key string) {
	fmt.Printf("trying to retrieve %s from the server\n", key)

	var value string
	if err := call("localhost:3410", "Node.Get", key, &value); err != nil {
		log.Fatalf("Error in 'call' function in the 'get' command: %v", err)
	}

	fmt.Println(value)
}

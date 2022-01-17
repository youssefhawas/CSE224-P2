package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func handleConnection(conn net.Conn, ch chan<- []byte) {
	buffer := make([]byte, 100)
	bytes, err := conn.Read(buffer)
	if err != nil {
		log.Panicln(err)
	}
	fmt.Print("Received from")
	fmt.Print(conn)
	fmt.Println(buffer[0:bytes])
	ch <- buffer[0:bytes]
}

func listenforData(ch chan<- []byte, serverid int) {
	listener, err := net.Listen("tcp", "server"+strconv.Itoa(serverid)+":8080")
	if err != nil {
		log.Panic(err)
	}
	fmt.Println("Server UP")
	defer listener.Close()

	time.Sleep(1 * time.Second)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Panic(err)
		}
		fmt.Println("Accepted connection from: ")
		fmt.Println(conn)
		go handleConnection(conn, ch)
	}
}

func sendData(conn net.Conn) {
	valbytes := []byte(strconv.Itoa(12))
	conn.Write(valbytes)
}

func dialToServers(serverId int, scs ServerConfigs) {
	time.Sleep(1 * time.Second)
	for _, serv := range scs.Servers {
		if serv.Host == ("server" + strconv.Itoa(serverId)) {
			continue
		} else {
			for {
				conn, err := net.Dial("tcp", serv.Host+":"+serv.Port)
				if err == nil {
					go sendData(conn)
					break
				}
				defer conn.Close()
			}
		}
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	ch := make(chan []byte)
	// records := [][]byte{}
	print("going to listen")
	go listenforData(ch, serverId)
	go dialToServers(serverId, scs)
	/*
		Implement Distributed Sort
	*/
	time.Sleep(100 * time.Second)
}

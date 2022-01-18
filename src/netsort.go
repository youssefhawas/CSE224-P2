package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
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
	for {
		buffer := make([]byte, 100)
		bytes, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				fmt.Println("EOF")
				conn.Close()
				break
			} else {
				log.Panicln(err)
			}
		}
		fmt.Print("Received from ")
		fmt.Print(conn)
		fmt.Println(strconv.Itoa(bytes) + " bytes")
		ch <- buffer[0:bytes]
	}
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

func sendData(conn net.Conn, send_data [][]byte) {
	all_zero_record := make([]byte, 100)
	for i, _ := range all_zero_record {
		all_zero_record[i] = byte(0)
	}
	for _, record := range send_data {
		conn.Write(record)
	}
	conn.Write(all_zero_record)
	time.Sleep(100 * time.Millisecond)
	conn.Close()
}

func dialToServers(serverId int, scs ServerConfigs, partition_map map[int][][]byte) {
	time.Sleep(1 * time.Second)
	for _, serv := range scs.Servers {
		send_data := partition_map[serv.ServerId]
		if serv.Host == ("server" + strconv.Itoa(serverId)) {
			continue
		} else {
			for {
				conn, err := net.Dial("tcp", serv.Host+":"+serv.Port)
				if err == nil {
					go sendData(conn, send_data)
					break
				}
				defer conn.Close()
			}
		}
	}
}

func consolidateData(ch <-chan []byte, numOfClients int) [][]byte {
	all_zero_record := make([]byte, 100)
	records := [][]byte{}
	for i, _ := range all_zero_record {
		all_zero_record[i] = byte(0)
	}
	done_count := 0
	for {
		record := <-ch
		// break if you receive flag records from all clients
		if bytes.Equal(all_zero_record, record) {
			done_count += 1
		} else {
			records = append(records, record)
		}
		if done_count == numOfClients {
			break
		}
	}
	return records
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

	// Read infile
	var infile string = os.Args[2]
	var outfile string = os.Args[3]
	f, err := os.Open(infile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	records := [][]byte{}
	for {
		buf := make([]byte, 100)
		n, err := f.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}
		records = append(records, buf[0:n])
	}
	fmt.Println("Read input file")

	n := int(math.Ceil(math.Log2(float64(len(scs.Servers)))))
	partition_map := make(map[int][][]byte)
	for _, serv := range scs.Servers {
		for _, record := range records {
			data := int(record[0] >> (8 - n))
			if data == serv.ServerId {
				partition_map[serv.ServerId] = append(partition_map[serv.ServerId], record)
			}
		}
	}
	fmt.Println("Created partition map")

	ch := make(chan []byte)
	// records := [][]byte{}
	go listenforData(ch, serverId)
	go dialToServers(serverId, scs, partition_map)
	/*
		Implement Distributed Sort
	*/
	numOfClients := len(scs.Servers) - 1
	received_records := consolidateData(ch, numOfClients)
	received_records = append(received_records, partition_map[serverId]...)

	sort.Slice(received_records, func(i, j int) bool { return bytes.Compare(received_records[i][:10], received_records[j][:10]) < 0 })

	fmt.Println(received_records)

	f, create_err := os.Create(outfile)
	if create_err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	for _, received_records := range received_records {
		_, write_err := f.Write(received_records)
		if write_err != nil {
			log.Fatal(err)
		}
	}

	f.Sync()
}

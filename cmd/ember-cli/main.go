package main

import (
	"bufio"
	"fmt"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/shlex"

	resp "github.com/Fusl/go-resp"
	"github.com/bytechan/resp3"
)

type tcp struct {
	host       string
	sourceAddr string
	port       string
}

type redisContext struct {
	TCP *tcp
}

const CLUSTER_CLI_SLOTS = 16384

type cliNode struct {
	ctx        redisContext
	port       string
	busPort    string
	dirty      int
	slots      [CLUSTER_CLI_SLOTS]uint8
	slotsCount int
	conn       net.Conn
}

func connect(port int) {
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	rconn := resp.NewServer(conn)
	defer rconn.Close()

	reader := resp3.NewReader(conn)

	// Ctrl-C / SIGTERM
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	// Read stdin in a separate goroutine so main can also listen for Ctrl-C.
	input := make(chan string)

	go func() {
		scanner := bufio.NewScanner(os.Stdin)

		for scanner.Scan() {
			input <- scanner.Text()
		}

		close(input)
	}()

	output := make(chan string)

	go func() {

		for {
			v, _, err := reader.ReadValue()
			if err != nil {
				fmt.Println("read error:", err)
				return
			}

			output <- fmt.Sprint(v.SmartResult())
		}
	}()

	fmt.Printf("Connected to EmberDB on 127.0.0.1:%d \n", port)

	for {
		fmt.Print("> ")

		select {
		case <-sig:
			fmt.Println("\nbye")
			return

		case line, ok := <-input:
			if !ok {
				return
			}

			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			if strings.EqualFold(line, "quit") {
				return
			}

			args, err := shlex.Split(line)
			if err != nil {
				fmt.Println("parse error:", err)
				continue
			}

			if err := rconn.WriteArrayString(args); err != nil {
				fmt.Println("write error:", err)
				return
			}
		case message := <-output:
			fmt.Printf("%s\n", message)
		}

	}
}

/*
parse supplied node addresses
↓
connect to every node
↓
validate all of them BEFORE mutating any
↓
build temporary CLI-side node representations
↓
compute slot allocation
↓
send CLUSTER ADDSLOTS-style commands to each server
*/
func main() {

	if len(os.Args) == 1 {
		connect(6379)
	}

	switch os.Args[1] {

	case "--create-cluster":

		var addrs []string

		if len(os.Args) == 2 {
			fmt.Println("No ports mentioned")
			return
		}

		for i := 2; i < len(os.Args); i++ {
			addrs = append(addrs, os.Args[i])
		}

		// try connecting to each port (check if they exist)
		// if yes to all N, proceed to cluster methods to create in-memory representation of these nodes

		var cliNodeArray []*cliNode

		for i := range len(addrs) {

			var node cliNode

			addr := addrs[i]
			host, port, err := net.SplitHostPort(addr)

			if err != nil {
				fmt.Printf("Error resolving port or host, %s\n", err)
				return
			}

			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)

			if err != nil {
				fmt.Printf("Error connecting to %s\n", addr)
				return
			}

			defer conn.Close()

			var tcp tcp
			tcp.host = host
			tcp.port = port
			tcp.sourceAddr = addr

			node.ctx.TCP = &tcp
			node.conn = conn

			cliNodeArray = append(cliNodeArray, &node)

			fmt.Printf("Successfully connected to %s\n", addr)
		}

		slotsPerNode := CLUSTER_CLI_SLOTS / float64(len(cliNodeArray))
		var first int64 = 0
		var cursor float64 = 0.0

		// loop over nodes and assign slots. these slots exist only in CLI-side state, so they need to be permeated to the server processes themselves
		for i := range len(cliNodeArray) {
			curNode := cliNodeArray[i]

			var last int64 = int64(math.Round(cursor + slotsPerNode - 1))

			if last > CLUSTER_CLI_SLOTS || i == len(cliNodeArray)-1 {
				last = CLUSTER_CLI_SLOTS - 1
			}
			if last < first {
				last = first
			}

			fmt.Printf("Master[%d] -> Slots %d - %d\n", i, first, last)

			curNode.slotsCount = 0
			for j := first; j <= last; j++ {
				curNode.slots[j] = 1
				curNode.slotsCount++
			}

			curNode.dirty = 1
			first = last + 1
			cursor += slotsPerNode
		}

		first = 0
		cursor = 0.0
		for i := range len(cliNodeArray) {

			curNode := cliNodeArray[i]
			if curNode.dirty != 1 {
				continue
			}

			var last int64 = int64(math.Round(cursor + slotsPerNode - 1))

			if last > CLUSTER_CLI_SLOTS || i == len(cliNodeArray)-1 {
				last = CLUSTER_CLI_SLOTS - 1
			}
			if last < first {
				last = first
			}

			rconn := resp.NewServer(curNode.conn)
			reader := resp3.NewReader(curNode.conn)

			rconn.WriteArrayString([]string{
				"CLUSTER",
				"ADDSLOTSRANGE",
				strconv.Itoa(int(first)),
				strconv.Itoa(int(last)),
			})

			v, _, err := reader.ReadValue()
			if err != nil {
				panic(err)
			}

			result := v.SmartResult()

			switch r := result.(type) {
			case string:
				if r == "OK" {
					curNode.dirty = 0
					fmt.Printf("Node %s configured successfully\n", curNode.ctx.TCP.sourceAddr)
				} else {
					fmt.Printf("unexpected response: %s\n", r)
				}
			case error:
				fmt.Printf("server rejected command: %v\n", r)

			default:
				fmt.Printf("unexpected response: %#v\n", r)

			}

			first = last + 1
			cursor += slotsPerNode
		}

	case "--port":
		if len(os.Args) != 3 {
			fmt.Println("No port mentioned, invalid command")
			return
		}

		port, err := strconv.Atoi(os.Args[2])
		if err != nil {
			return
		}

		connect(port)

	default:
	}
}

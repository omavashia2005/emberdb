package main

import (
	"bufio"
	"fmt"
	"github.com/google/shlex"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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
	slots      [CLUSTER_CLI_SLOTS][]uint8
	slotsCount int
	conn       net.Conn
}

func main() {

	if len(os.Args) > 1 && os.Args[1] == "--create-cluster" {

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

		var cliNodeArray []cliNode

		for i := range len(addrs) {

			var node cliNode
			addr := addrs[i]
			host, port, err := net.SplitHostPort(addr)

			fmt.Println(host)
			fmt.Println(port)

			if err != nil {
				fmt.Printf("Error resolving port or host, %s\n", err)
				return
			}

			var tcp tcp

			tcp.host = host
			tcp.port = port

			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)

			if err != nil {
				fmt.Printf("Error connecting to %s\n", addr)
				return
			}

			defer conn.Close()

			tcp.sourceAddr = addr
			node.ctx.TCP = &tcp
			node.conn = conn

			cliNodeArray = append(cliNodeArray, node)

			fmt.Printf("Successfully connected to %s\n", addr)
		}

		// loop over nodes and assign slots. these slots exist only in CLI-side state, so they need to be permeated to the server processes themselves

		// to permeate these, implement another client-side command named CLUSTER ADDSLOTS and send appropriate commands to required nodes

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

		return
	}

	conn, err := net.Dial("tcp", "127.0.0.1:6379")
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

	fmt.Println("Connected to EmberDB on 127.0.0.1:6739")

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

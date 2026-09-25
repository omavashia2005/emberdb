package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/shlex"
	"github.com/omavashia2005/emberdb/utils"
	"github.com/omavashia2005/emberdb/utils/clusters"

	resp "github.com/Fusl/go-resp"
	"github.com/bytechan/resp3"
	// server "github.com/omavashia2005/emberdb/cmd/ember-server"
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
	ctx         redisContext
	port        string
	busPort     string
	dirty       int
	slots       [CLUSTER_CLI_SLOTS]uint8
	slotsCount  int
	conn        net.Conn
	clusterHost string
	clusterPort int
}

func readClusterNodes(addr string) (map[string]*clusters.ClusterNode, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("%w: connect to %s: %v", utils.ErrConnection, addr, err)
	}
	defer conn.Close()

	rconn := resp.NewServer(conn)
	if err := rconn.WriteArrayString([]string{"CLUSTER", "NODES"}); err != nil {
		return nil, fmt.Errorf("%w: send CLUSTER NODES to %s: %v", utils.ErrConnection, addr, err)
	}
	v, _, err := resp3.NewReader(conn).ReadValue()
	if err != nil {
		return nil, fmt.Errorf("%w: read CLUSTER NODES from %s: %v", utils.ErrConnection, addr, err)
	}
	payload, ok := v.SmartResult().(string)
	if !ok {
		return nil, fmt.Errorf("%w: CLUSTER NODES from %s returned %T, want string", utils.ErrCluster, addr, v.SmartResult())
	}
	if !strings.HasPrefix(strings.TrimSpace(payload), "{") {
		return nil, fmt.Errorf("%w: server %s rejected CLUSTER NODES: %s", utils.ErrCluster, addr, payload)
	}

	var nodes map[string]*clusters.ClusterNode
	if err := json.Unmarshal([]byte(payload), &nodes); err != nil {
		return nil, fmt.Errorf("%w: decode CLUSTER NODES from %s: %v", utils.ErrCluster, addr, err)
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("%w: CLUSTER NODES returned no nodes", utils.ErrCluster)
	}
	for name, node := range nodes {
		if node == nil {
			return nil, fmt.Errorf("%w: CLUSTER NODES returned nil node %q", utils.ErrCluster, name)
		}
		if node.Name == "" || node.ClientPort < 1 || node.ClientPort > 65535 {
			return nil, fmt.Errorf("%w: CLUSTER NODES returned invalid node %q (name=%q, port=%d)", utils.ErrCluster, name, node.Name, node.ClientPort)
		}
	}
	return nodes, nil
}

func connect(port int) {
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		utils.PrintError(fmt.Errorf("%w: %v", utils.ErrConnection, err))
		return
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
				utils.PrintError(fmt.Errorf("%w: read response: %v", utils.ErrConnection, err))
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
				utils.PrintError(fmt.Errorf("%w: %v", utils.ErrInvalidInput, err))
				continue
			}

			if err := rconn.WriteArrayString(args); err != nil {
				utils.PrintError(fmt.Errorf("%w: write command: %v", utils.ErrConnection, err))
				return
			}
		case message := <-output:
			fmt.Printf("%s\n", message)
		}

	}
}

func main() {

	if len(os.Args) == 1 {
		connect(6379)
		return
	}

	switch os.Args[1] {

	case "--cluster-add-node":
		docker := len(os.Args) > 2 && os.Args[2] == "--docker"
		offset := 2
		if docker {
			offset++
		}
		if len(os.Args) != offset+2 {
			fmt.Println("Usage: --cluster-add-node [--docker] <new-host>:<port> <existing-host>:<port>")
			return
		}

		newNodeHost, newNodePort, err := net.SplitHostPort(os.Args[offset])
		if err != nil {
			utils.PrintError(fmt.Errorf("%w: new node: %v", utils.ErrInvalidInput, err))
			return
		}
		existingNodeHost, existingNodePort, err := net.SplitHostPort(os.Args[offset+1])
		if err != nil {
			utils.PrintError(fmt.Errorf("%w: existing node: %v", utils.ErrInvalidInput, err))
			return
		}

		removeContainer := false
		if docker {
			containerName := newNodeHost
			cmd := exec.Command(
				"docker", "compose", "run",
				"-d",
				"--name", newNodeHost,
				"-p", newNodePort+":6379",
				"node-1",
				"__node", "6379", newNodeHost,
			)

			output, err := cmd.CombinedOutput()
			if err != nil {
				utils.PrintError(fmt.Errorf("%w: start Docker node: %v: %s", utils.ErrStartup, err, output))
				return
			}
			removeContainer = true
			defer func() {
				if removeContainer {
					_ = exec.Command("docker", "rm", "-f", containerName).Run()
				}
			}()
			newNodeHost = "127.0.0.1"
		}

		existingPort, err := strconv.Atoi(existingNodePort)
		if err != nil {
			utils.PrintError(fmt.Errorf("%w: existing node port: %v", utils.ErrInvalidInput, err))
			return
		}
		newNodeConn, err := net.Dial("tcp", net.JoinHostPort(newNodeHost, newNodePort))
		if err != nil {
			utils.PrintError(fmt.Errorf("%w: connect to new node: %v", utils.ErrConnection, err))
			return
		}
		defer newNodeConn.Close()

		err = clusters.ClusterMeet(
			newNodeConn,
			existingPort,
			existingNodeHost,
		)
		if err != nil {
			utils.PrintError(fmt.Errorf("%w: add node: %v", utils.ErrCluster, err))
			return
		}
		if docker {
			time.Sleep(10 * time.Second)
		}
		removeContainer = false

	case "--cluster-rebalance-nodes", "--rebalance-nodes":
		addr := "127.0.0.1:6379"
		if len(os.Args) > 3 {
			fmt.Println("Usage: --rebalance-nodes [host:port]")
			return
		}
		if len(os.Args) == 3 && os.Args[2] != "" {
			addr = os.Args[2]
		}
		host, _, err := net.SplitHostPort(addr)
		if err == nil && host == "" {
			err = fmt.Errorf("host is empty")
		}
		if err != nil {
			utils.PrintError(fmt.Errorf("%w: rebalance node address %q: %v", utils.ErrInvalidInput, addr, err))
			return
		}
		nodes, err := readClusterNodes(addr)
		if err != nil {
			utils.PrintError(err)
			return
		}
		result, err := clusters.ClusterRebalanceNodes(nodes)
		if err != nil {
			utils.PrintError(fmt.Errorf("%w: rebalance: %v", utils.ErrCluster, err))
			return
		}
		if result != 1 {
			utils.PrintError(fmt.Errorf("%w: rebalance returned %d", utils.ErrCluster, result))
			return
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
	case "--create-cluster":

		var addrs []string

		if len(os.Args) == 2 {
			fmt.Println("No ports mentioned")
			return
		}

		for i := 2; i < len(os.Args); i++ {
			addrs = append(addrs, os.Args[i])
		}

		if len(addrs) < 3 {
			utils.PrintError(fmt.Errorf("%w: at least 3 nodes needed to create cluster", utils.ErrInvalidInput))
			return
		}

		// try connecting to each port (check if they exist)
		// if yes to all N, proceed to cluster methods to create in-memory representation of these nodes
		var cliNodeArray []*cliNode

		for i := range len(addrs) {

			var node cliNode

			addr := addrs[i]
			host, port, err := net.SplitHostPort(addr)

			if err != nil {
				utils.PrintError(fmt.Errorf("%w: node address %q: %v", utils.ErrInvalidInput, addr, err))
				return
			}

			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)

			if err != nil {
				utils.PrintError(fmt.Errorf("%w: connect to %s: %v", utils.ErrConnection, addr, err))
				return
			}

			defer conn.Close()

			var tcp tcp
			tcp.host = host
			tcp.port = port
			tcp.sourceAddr = addr

			node.ctx.TCP = &tcp
			node.conn = conn

			rconn := resp.NewServer(conn)
			reader := resp3.NewReader(conn)

			err = rconn.WriteArrayString([]string{
				"CLUSTER",
				"MYADDR",
			})

			if err != nil {
				utils.PrintError(fmt.Errorf("%w: request CLUSTER MYADDR from %s: %v", utils.ErrConnection, addr, err))
				return
			}

			v, _, err := reader.ReadValue()
			if err != nil {
				utils.PrintError(fmt.Errorf("%w: read CLUSTER MYADDR from %s: %v", utils.ErrConnection, addr, err))
				return
			}

			result := v.SmartResult()
			values, ok := result.([]interface{})
			if !ok || len(values) != 2 {
				utils.PrintError(fmt.Errorf("%w: unexpected CLUSTER MYADDR response: %#v", utils.ErrCluster, result))
				return
			}

			node.clusterHost = fmt.Sprint(values[0])

			node.clusterPort, err = strconv.Atoi(fmt.Sprint(values[1]))
			if err != nil {
				utils.PrintError(fmt.Errorf("%w: invalid CLUSTER MYADDR port: %v", utils.ErrCluster, err))
				return
			}

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

			if err := utils.ExpectStringResponse(reader, "OK"); err != nil {
				fmt.Println(err)
			} else {
				curNode.dirty = 0
				fmt.Printf("Node %s configured successfully\n", curNode.ctx.TCP.sourceAddr)
			}

			first = last + 1
			cursor += slotsPerNode
		}

		bootstrapConn := cliNodeArray[0].conn

		for i := 1; i < len(cliNodeArray); i++ {
			target := cliNodeArray[i]

			err := clusters.ClusterMeet(
				bootstrapConn,
				target.clusterPort,
				target.clusterHost,
			)
			if err != nil {
				utils.PrintError(fmt.Errorf("%w: meet %s: %v", utils.ErrCluster, target.ctx.TCP.sourceAddr, err))
				return
			}
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

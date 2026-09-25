package main

import (
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bytechan/resp3"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

type workload struct {
	name  string
	setup func(string) []string
	args  func(string) []string
}

type result struct {
	command string
	ember   float64
	redis   float64
}

type benchClient struct {
	conn   net.Conn
	writer *resp3.Writer
	reader *resp3.Reader
	tag    string
}

var workloads = []workload{
	{
		name: "SET",
		args: func(tag string) []string { return []string{"SET", key("set", tag), "xxx"} },
	},
	{
		name:  "GET",
		setup: func(tag string) []string { return []string{"SET", key("get", tag), "xxx"} },
		args:  func(tag string) []string { return []string{"GET", key("get", tag)} },
	},
	{
		name: "MSET_10",
		args: msetArgs,
	},
	{
		name:  "MGET_10",
		setup: msetArgs,
		args:  mgetArgs,
	},
}

func main() {
	requests := flag.Int("requests", 100_000, "requests per workload and product")
	clients := flag.Int("clients", 50, "concurrent clients")
	emberStandalone := flag.String("ember-standalone", "", "EmberDB standalone address")
	redisStandalone := flag.String("redis-standalone", "", "Redis standalone address")
	emberCluster := flag.String("ember-cluster", "", "comma-separated EmberDB cluster addresses")
	redisCluster := flag.String("redis-cluster", "", "comma-separated Redis cluster addresses")
	flag.Parse()

	if *requests < 1 || *clients < 1 {
		fmt.Fprintln(os.Stderr, "requests and clients must be positive")
		os.Exit(2)
	}

	fmt.Printf("Docker benchmark comparison: identical Go RESP client, %d requests, %d clients\n", *requests, *clients)
	if err := compare("STANDALONE", split(*emberStandalone), split(*redisStandalone), *requests, *clients); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := compare("3-NODE DOCKER CLUSTER", split(*emberCluster), split(*redisCluster), *requests, *clients); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func compare(mode string, emberAddrs, redisAddrs []string, requests, clients int) error {
	if len(emberAddrs) == 0 || len(redisAddrs) == 0 || len(emberAddrs) != len(redisAddrs) {
		return fmt.Errorf("%s requires matching EmberDB and Redis addresses", mode)
	}

	results := make([]result, 0, len(workloads))
	for _, work := range workloads {
		ember, err := measure(emberAddrs, work, requests, clients)
		if err != nil {
			return fmt.Errorf("%s EmberDB %s: %w", mode, work.name, err)
		}
		redis, err := measure(redisAddrs, work, requests, clients)
		if err != nil {
			return fmt.Errorf("%s Redis %s: %w", mode, work.name, err)
		}
		results = append(results, result{command: work.name, ember: ember, redis: redis})
	}

	fmt.Printf("\n=== %s: EMBERDB vs REDIS ===\n", mode)
	fmt.Printf("%-12s %18s %18s %14s\n", "COMMAND", "EMBERDB ops/s", "REDIS ops/s", "REDIS/EMBER")
	for _, result := range results {
		fmt.Printf("%-12s %18.0f %18.0f %13.2fx\n", result.command, result.ember, result.redis, result.redis/result.ember)
	}
	return nil
}

func measure(addrs []string, work workload, requests, clients int) (float64, error) {
	tags := tagsForClients(len(addrs), clients)
	if work.setup != nil {
		for i, tag := range tags {
			if err := runOnce(addrs[i%len(addrs)], work.setup(tag)); err != nil {
				return 0, err
			}
		}
	}

	connections := make([]benchClient, clients)
	for i := range connections {
		conn, err := net.Dial("tcp", addrs[i%len(addrs)])
		if err != nil {
			closeClients(connections)
			return 0, err
		}
		connections[i] = benchClient{conn: conn, writer: resp3.NewWriter(conn), reader: resp3.NewReader(conn), tag: tags[i]}
	}
	defer closeClients(connections)

	start := make(chan struct{})
	errs := make(chan error, clients)
	var wg sync.WaitGroup
	base, extra := requests/clients, requests%clients
	for i := range connections {
		count := base
		if i < extra {
			count++
		}
		wg.Add(1)
		go func(c benchClient, count int) {
			defer wg.Done()
			<-start
			args := work.args(c.tag)
			for range count {
				if err := c.writer.WriteCommand(args...); err != nil {
					errs <- err
					return
				}
				value, _, err := c.reader.ReadValue()
				if err != nil {
					errs <- err
					return
				}
				if value.Err != "" {
					errs <- fmt.Errorf("server error: %s", value.Err)
					return
				}
			}
		}(connections[i], count)
	}

	started := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(started)
	close(errs)
	if err := <-errs; err != nil {
		return 0, err
	}
	return float64(requests) / elapsed.Seconds(), nil
}

func runOnce(addr string, args []string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := resp3.NewWriter(conn).WriteCommand(args...); err != nil {
		return err
	}
	value, _, err := resp3.NewReader(conn).ReadValue()
	if err == nil && value.Err != "" {
		return fmt.Errorf("server error: %s", value.Err)
	}
	return err
}

func tagsForClients(nodes, clients int) []string {
	tags := make([]string, clients)
	for client := range clients {
		node := client % nodes
		start := int(math.Round(float64(node) * 16384 / float64(nodes)))
		end := int(math.Round(float64(node+1)*16384/float64(nodes) - 1))
		if node == nodes-1 {
			end = 16383
		}
		for candidate := 0; ; candidate++ {
			tag := strconv.Itoa(client) + ":" + strconv.Itoa(candidate)
			slot := int(kvstore.SlotForKey("{" + tag + "}"))
			if slot >= start && slot <= end {
				tags[client] = tag
				break
			}
		}
	}
	return tags
}

func key(prefix, tag string) string {
	return "bench:" + prefix + "{" + tag + "}"
}

func msetArgs(tag string) []string {
	args := []string{"MSET"}
	for i := range 10 {
		args = append(args, key("multi:"+strconv.Itoa(i), tag), "xxx")
	}
	return args
}

func mgetArgs(tag string) []string {
	args := []string{"MGET"}
	for i := range 10 {
		args = append(args, key("multi:"+strconv.Itoa(i), tag))
	}
	return args
}

func split(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

func closeClients(clients []benchClient) {
	for _, client := range clients {
		if client.conn != nil {
			_ = client.conn.Close()
		}
	}
}

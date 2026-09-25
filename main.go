package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	server "github.com/omavashia2005/emberdb/cmd/ember-server"
	"github.com/omavashia2005/emberdb/utils"
	// "github.com/omavashia2005/emberdb/utils/clusters"
)

var CLUSTER_NODES []*exec.Cmd
var CLUSTER_PORTS = []string{"6379", "6380", "6381"}

func startCluster() error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("%w: find executable: %v", utils.ErrStartup, err)
	}

	for _, port := range CLUSTER_PORTS {
		cmd := exec.Command(exe, "__node", port, "127.0.0.1")

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			return fmt.Errorf(
				"%w: start node on %s: %v",
				utils.ErrStartup,
				port,
				err,
			)
		}

		CLUSTER_NODES = append(CLUSTER_NODES, cmd)

		fmt.Printf(
			"started node on port %s [PID %d]\n",
			port,
			cmd.Process.Pid,
		)
	}

	return nil
}

func main() {
	if len(os.Args) == 3 && os.Args[1] == "__standalone" {
		server.Run(os.Args[2], "", false)
		return
	}

	if len(os.Args) == 4 && os.Args[1] == "__node" {
		port := os.Args[2]
		clusterHost := os.Args[3]

		server.Run(port, clusterHost, true)
		return
	}

	cleanup := func() {
		for _, node := range CLUSTER_NODES {
			if node.Process != nil {
				_ = node.Process.Kill()
				_, _ = node.Process.Wait()
			}
		}

		cmd := exec.Command(
			"docker",
			"compose",
			"down",
			"--remove-orphans",
		)

		if err := cmd.Run(); err != nil {
			utils.PrintError(err)
		}
	}

	input := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)

		for scanner.Scan() {
			input <- scanner.Text()
		}

		close(input)

	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	for {

		fmt.Print("ember-server> ")
		select {

		case <-sig:
			fmt.Println("\nshutting down")
			cleanup()
			return

		case line, ok := <-input:

			if !ok {
				cleanup()
				return
			}

			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			if strings.EqualFold(line, "quit") {
				cleanup()
				return
			}

			args := strings.Fields(line)

			if len(args) == 2 {
				if args[0] != "ember" {
					fmt.Printf("Invalid command: %s\n", args[0])
					continue
				}
				switch args[1] {
				case "start":
					fmt.Println("ember running on port 6379")
					go server.Run("6379", "", false)
					continue

				case "cluster-start":
					err := startCluster()
					if err != nil {
						utils.PrintError(err)
					}
					continue
				default:
					fmt.Printf("No such option: %s\n", args[1])
					continue
				}

			} else if len(args) == 3 {

				switch args[1] {
				case "port":
					port := args[2]
					fmt.Printf("ember running on port %s\n", port)

					go server.Run(port, "", false)
					continue
				default:
					fmt.Printf("No such option: %s\n", args[1])
					continue

				}

			}

		}

	}
}

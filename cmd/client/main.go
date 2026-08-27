package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	resp "github.com/Fusl/go-resp"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:6739")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	rconn := resp.NewServer(conn)
	defer rconn.Close()

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

		buf := make([]byte, 4096)

		for {
			n, err := conn.Read(buf)
			if err != nil {
				fmt.Println("read error:", err)
				return
			}

			output <- string(buf[:n])
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

			args := strings.Fields(line)

			if err := rconn.WriteArrayString(args); err != nil {
				fmt.Println("write error:", err)
				return
			}
		case message := <-output:
			fmt.Printf("%s", message)
		}

	}
}

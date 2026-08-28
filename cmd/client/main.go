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
	"github.com/bytechan/resp3"
)

func main() {
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

			args := strings.Fields(line)

			if err := rconn.WriteArrayString(args); err != nil {
				fmt.Println("write error:", err)
				return
			}
		case message := <-output:
			fmt.Printf("%s\n", message)
		}

	}
}

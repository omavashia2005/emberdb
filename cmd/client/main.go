package main

import (
	"bufio"
	"fmt"
	resp "github.com/Fusl/go-resp"
	"net"
	"os"
	"strings"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:6739")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	rconn := resp.NewServer(conn)
	defer rconn.Close()
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("> ")
	for scanner.Scan() {
		line := scanner.Text()
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

		// For now: read raw RESP response.
		buf := make([]byte, 4096)

		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("read error:", err)
			return
		}

		fmt.Printf("%s", buf[:n])
		fmt.Print("> ")
	}
}

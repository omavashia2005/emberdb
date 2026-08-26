package main

import (
	"fmt"
	"log"
	"net"

	"github.com/Fusl/go-resp"
)

func main() {
	conn, err := net.Dial("tcp", ":6739")
	if err != nil {
		panic(err)
	}
	rconn := resp.NewServer(conn)
	defer conn.Close()
	defer rconn.Close()

	log.Printf("Connected to server on 6739")


	command := []byte("ECHO om\r\n")
	n, err := conn.Write(command)
	if err != nil {
		panic(err)
	}

	fmt.Println("wrote", n, "bytes")

	buf := make([]byte, 8192)

	m, err := conn.Read(buf)
	fmt.Printf("%q", m)
	fmt.Printf("%q\n", buf[:m])
	if err != nil {
		panic(err)
	}

}

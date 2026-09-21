package clusters

import (
	"encoding/binary"
	"io"
	"net"
	"time"
)

type clusterLink struct {
	connection net.Conn
	node       *ClusterNode
	inbound    bool
	ctime      time.Time
	send       chan []byte
}

func newClusterLink(conn net.Conn, node *ClusterNode, inbound bool) *clusterLink {
	return &clusterLink{
		connection: conn,
		node:       node,
		inbound:    inbound,
		ctime:      time.Now(),
		send:       make(chan []byte, 256),
	}
}

func (l *clusterLink) GetConnection() net.Conn   { return l.connection }
func (l *clusterLink) GetNode() *ClusterNode     { return l.node }
func (l *clusterLink) SetNode(node *ClusterNode) { l.node = node }
func (l *clusterLink) IsInbound() bool           { return l.inbound }
func (l *clusterLink) Send() chan []byte         { return l.send }

func writeFull(conn net.Conn, buf []byte) error {
	for len(buf) > 0 {
		n, err := conn.Write(buf)
		if err != nil {
			return err
		}

		buf = buf[n:]
	}

	return nil
}
func clusterReadLoop(link *clusterLink) {

	for {
		prefix := make([]byte, 8)

		_, err := io.ReadFull(link.GetConnection(), prefix)
		if err != nil {
			return
		}

		if string(prefix[:4]) != "RCmb" {
			return
		}

		totLen := binary.BigEndian.Uint32(prefix[4:8])
		buf := make([]byte, int(totLen))

		copy(buf[:8], prefix)

		_, err = io.ReadFull(link.GetConnection(), buf[8:])
		if err != nil {
			return
		}

		msg, err := deserializeClusterMsg(buf)

		clusterProcessMsg(link, msg)

	}
}

func clusterWriteLoop(link *clusterLink) {
	for buf := range link.Send() {
		if err := writeFull(link.GetConnection(), buf); err != nil {
			return
		}
	}
}

package pubsub

import (
	"strconv"

	"github.com/panjf2000/gnet/v2"
)

// PubSub is accessed only on the event loop (SUBSCRIBE/PUBLISH from command
// dispatch, UNSUBSCRIBE from dispatch and OnClose), so it needs no lock. Fan-out
// to subscribers uses gnet's concurrency-safe AsyncWrite, which schedules the
// write on the subscriber conn's loop.
type PubSub struct {
	subscriptions map[string]map[gnet.Conn]struct{}
}

func NewPubSub() *PubSub {
	return &PubSub{subscriptions: make(map[string]map[gnet.Conn]struct{})}
}

func Subscribe(channel string, c gnet.Conn, ps *PubSub) {
	subs := ps.subscriptions[channel]
	if subs == nil {
		subs = make(map[gnet.Conn]struct{})
		ps.subscriptions[channel] = subs
	}
	subs[c] = struct{}{}
}

func Unsubscribe(channel string, c gnet.Conn, ps *PubSub) {
	subs := ps.subscriptions[channel]
	if subs == nil {
		return
	}
	delete(subs, c)
	if len(subs) == 0 {
		delete(ps.subscriptions, channel)
	}
}

// Publish sends message as a RESP bulk string to every subscriber of channel and
// returns the number of subscribers it was delivered to.
func Publish(channel string, message string, ps *PubSub) int {
	subs := ps.subscriptions[channel]
	if len(subs) == 0 {
		return 0
	}

	// $<len>\r\n<message>\r\n — matches the previous bulk-string delivery.
	frame := make([]byte, 0, len(message)+16)
	frame = append(frame, '$')
	frame = strconv.AppendInt(frame, int64(len(message)), 10)
	frame = append(frame, '\r', '\n')
	frame = append(frame, message...)
	frame = append(frame, '\r', '\n')

	count := 0
	for c := range subs {
		if err := c.AsyncWrite(frame, nil); err == nil {
			count++
		}
	}
	return count
}

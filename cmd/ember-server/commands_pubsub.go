package server

import (
	"fmt"

	"github.com/Fusl/go-resp"
	"github.com/omavashia2005/emberdb/utils/pubsub"
)

func handlePublish(rconn *resp.Server, args [][]byte) {
	if len(args) != 2 {
		rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'PUBLISH' command"))
		return
	}

	channel, message := string(args[0]), string(args[1])
	rconn.WriteInt(pubsub.Publish(channel, message, ps))
}

func handleSubscribe(rconn *resp.Server, args [][]byte, subscriptions map[string][]chan string) {
	if len(args) < 1 {
		rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'SUBSCRIBE' command"))
		return
	}

	for i := range args {

		channel := string(args[i])
		ch := pubsub.Subscribe(channel, ps)
		subscriptions[channel] = append(subscriptions[channel], ch)

		go func() {

			for message := range ch {
				fmt.Printf("Received message on channel %s: %s\n", channel, message)
				rconn.WriteString(message)
			}

		}()

		rconn.WriteOK()
	}
}

func handleUnsubscribe(rconn *resp.Server, args [][]byte, subscriptions map[string][]chan string) {
	channels := make([]string, len(args))
	for i := range args {
		channels[i] = string(args[i])
	}
	if len(channels) == 0 {
		for channel := range subscriptions {
			channels = append(channels, channel)
		}
	}
	for _, channel := range channels {
		for _, subscriber := range subscriptions[channel] {
			pubsub.Unsubscribe(channel, subscriber, ps)
		}
		delete(subscriptions, channel)
	}
	rconn.WriteOK()
}

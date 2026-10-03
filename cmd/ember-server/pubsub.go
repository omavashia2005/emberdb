package server

import (
	"fmt"

	"github.com/omavashia2005/emberdb/utils/pubsub"
)

var ps = pubsub.NewPubSub()

func handlePublish(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 2 {
		c.rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'PUBLISH' command"))
		return true
	}

	channel, message := string(args[0]), string(args[1])
	c.rconn.WriteInt(pubsub.Publish(channel, message, ps))
	return true
}

func handleSubscribe(c *connection, cmd string, args [][]byte) bool {
	if len(args) < 1 {
		c.rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'SUBSCRIBE' command"))
		return true
	}

	for i := range args {

		channel := string(args[i])
		ch := pubsub.Subscribe(channel, ps)
		c.subscriptions[channel] = append(c.subscriptions[channel], ch)

		go func() {

			for message := range ch {
				fmt.Printf("Received message on channel %s: %s\n", channel, message)
				c.rconn.WriteString(message)
			}

		}()

		c.rconn.WriteOK()
	}
	return true
}

func handleUnsubscribe(c *connection, cmd string, args [][]byte) bool {
	channels := make([]string, len(args))
	for i := range args {
		channels[i] = string(args[i])
	}
	if len(channels) == 0 {
		for channel := range c.subscriptions {
			channels = append(channels, channel)
		}
	}
	for _, channel := range channels {
		for _, subscriber := range c.subscriptions[channel] {
			pubsub.Unsubscribe(channel, subscriber, ps)
		}
		delete(c.subscriptions, channel)
	}
	c.rconn.WriteOK()
	return true
}

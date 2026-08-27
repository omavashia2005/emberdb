package pubsub

import (
	"sync"
)

type Subscription struct {
	channel     string
	subscribers []chan<- string
}
type PubSub struct {
	subscriptions map[string]*Subscription
	mu            sync.RWMutex
}

func NewPubSub() *PubSub {
	return &PubSub{
		subscriptions: make(map[string]*Subscription),
	}
}

func Subscribe(channel string, ps *PubSub) chan string {

	ps.mu.Lock()
	defer ps.mu.Unlock()

	sub, ok := ps.subscriptions[channel]
	if !ok {
		sub = &Subscription{
			channel:     channel,
			subscribers: make([]chan<- string, 0),
		}

		ps.subscriptions[channel] = sub
	}

	ch := make(chan string, 1)
	sub.subscribers = append(sub.subscribers, ch)

	return ch
}

func Publish(channel string, message string, ps *PubSub) int {

	ps.mu.Lock()
	defer ps.mu.Unlock()

	subs, ok := ps.subscriptions[channel]
	count := 0
	if !ok {
		return count
	}

	for _, subscriber := range subs.subscribers {

		select {
		case subscriber <- message:
			count++
		default:
		}

	}
	return count
}

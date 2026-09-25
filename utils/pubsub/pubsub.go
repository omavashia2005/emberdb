package pubsub

import (
	"sync"
)

type Subscription struct {
	channel     string
	subscribers []chan string
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
			subscribers: make([]chan string, 0),
		}

		ps.subscriptions[channel] = sub
	}

	ch := make(chan string, 1)
	sub.subscribers = append(sub.subscribers, ch)

	return ch
}

func Unsubscribe(channel string, subscriber chan string, ps *PubSub) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	sub, ok := ps.subscriptions[channel]
	if !ok {
		return
	}
	for i, ch := range sub.subscribers {
		if ch == subscriber {
			sub.subscribers = append(sub.subscribers[:i], sub.subscribers[i+1:]...)
			close(ch)
			break
		}
	}
	if len(sub.subscribers) == 0 {
		delete(ps.subscriptions, channel)
	}
}

func UnsubscribeAll(ps *PubSub) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for _, sub := range ps.subscriptions {
		for _, ch := range sub.subscribers {
			close(ch)
		}
	}
	ps.subscriptions = make(map[string]*Subscription)
}

func Publish(channel string, message string, ps *PubSub) int {

	ps.mu.RLock()
	defer ps.mu.RUnlock()

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

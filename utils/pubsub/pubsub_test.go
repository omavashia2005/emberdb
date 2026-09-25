package pubsub

import "testing"

func TestUnsubscribeStopsDeliveryAndClosesSubscriber(t *testing.T) {
	ps := NewPubSub()
	subscriber := Subscribe("updates", ps)
	Unsubscribe("updates", subscriber, ps)

	if got := Publish("updates", "message", ps); got != 0 {
		t.Fatalf("Publish count = %d, want 0", got)
	}
	if _, open := <-subscriber; open {
		t.Fatal("subscriber channel remained open")
	}
}

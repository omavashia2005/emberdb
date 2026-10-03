package pubsub_test

import (
	"testing"

	"github.com/omavashia2005/emberdb/utils/pubsub"
)

func TestUnsubscribeStopsDeliveryAndClosesSubscriber(t *testing.T) {
	ps := pubsub.NewPubSub()
	subscriber := pubsub.Subscribe("updates", ps)
	pubsub.Unsubscribe("updates", subscriber, ps)

	if got := pubsub.Publish("updates", "message", ps); got != 0 {
		t.Fatalf("pubsub.Publish count = %d, want 0", got)
	}
	if _, open := <-subscriber; open {
		t.Fatal("subscriber channel remained open")
	}
}

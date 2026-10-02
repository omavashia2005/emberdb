package pubsub

import (
	"testing"

	"github.com/panjf2000/gnet/v2"
)

func TestUnsubscribeStopsDelivery(t *testing.T) {
	ps := NewPubSub()
	var c gnet.Conn // nil conn is a valid map key; never written to after unsubscribe
	Subscribe("updates", c, ps)
	Unsubscribe("updates", c, ps)

	if got := Publish("updates", "message", ps); got != 0 {
		t.Fatalf("Publish count = %d, want 0", got)
	}
}

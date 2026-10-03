package server

import (
	"fmt"
	"testing"
)

func TestDataTypeCommands(t *testing.T) {
	server := startCommandServer(t)

	if got := fmt.Sprint(server.run(t, command("LPUSH", "list", "c", "b", "a"))); got != "3" {
		t.Fatalf("LPUSH = %s", got)
	}
	if got := fmt.Sprint(server.run(t, command("LRANGE", "list", "0", "-1"))); got != "[c b a]" {
		t.Fatalf("LRANGE = %s", got)
	}

	server.run(t, command("HMSET", "hash", "one", "1", "two", "2"))
	if got := fmt.Sprint(server.run(t, command("HMGET", "hash", "two", "missing"))); got != "[2 <nil>]" {
		t.Fatalf("HMGET = %s", got)
	}

	if got := fmt.Sprint(server.run(t, command("SADD", "set", "a", "b", "a"))); got != "2" {
		t.Fatalf("SADD = %s", got)
	}
	if got := fmt.Sprint(server.run(t, command("SISMEMBER", "set", "b"))); got != "1" {
		t.Fatalf("SISMEMBER = %s", got)
	}

	server.run(t, command("ZADD", "sorted", "3", "c", "1", "a", "2", "b"))
	if got := fmt.Sprint(server.run(t, command("ZRANGE", "sorted", "0", "-1"))); got != "[a b c]" {
		t.Fatalf("ZRANGE = %s", got)
	}
}

func TestUnsubscribeCommandStopsDelivery(t *testing.T) {
	subscriber := startCommandServer(t)
	publisher := startCommandServer(t)
	subscriber.run(t, command("SUBSCRIBE", "updates"))
	if got := subscriber.run(t, command("UNSUBSCRIBE", "updates")); got != "OK" {
		t.Fatalf("UNSUBSCRIBE = %#v", got)
	}
	if got := fmt.Sprint(publisher.run(t, command("PUBLISH", "updates", "message"))); got != "0" {
		t.Fatalf("PUBLISH count = %s", got)
	}
}

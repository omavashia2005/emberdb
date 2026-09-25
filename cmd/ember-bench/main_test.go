package main

import (
	"testing"

	"github.com/omavashia2005/emberdb/utils/kvstore"
)

func TestTagsForClientsAreUniqueAndLocal(t *testing.T) {
	tags := tagsForClients(3, 50)
	seen := make(map[string]bool, len(tags))
	for client, tag := range tags {
		if seen[tag] {
			t.Fatalf("duplicate tag %q", tag)
		}
		seen[tag] = true

		slot := int(kvstore.SlotForKey("{" + tag + "}"))
		node := client % 3
		if (node == 0 && slot > 5460) || (node == 1 && (slot < 5461 || slot > 10922)) || (node == 2 && slot < 10923) {
			t.Fatalf("client %d tag %q hashes to slot %d on another node", client, tag, slot)
		}
	}
}

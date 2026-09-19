package utils

import (
	"fmt"
	"strings"
)

func progressBar(done, total, width int) string {
	if total < 1 {
		total = 1
	}
	if done < 0 {
		done = 0
	}
	if done > total {
		done = total
	}
	filled := done * width / total
	return strings.Repeat("#", filled) + strings.Repeat("-", width-filled)
}

func shortNodeName(name string) string {
	if len(name) > 8 {
		return name[:8]
	}
	return name
}

func ShowRebalanceProgress(source, target string, totalDone, totalSlots, nodeIndex, nodeSlots int, slot uint64, phase string) {
	if len(phase) > 20 {
		phase = phase[:20]
	}
	fmt.Printf("\r\033[2K[rebalance] total[%s] %d/%d node[%s] %d/%d %s>%s slot:%d %s",
		progressBar(totalDone, totalSlots, 12),
		totalDone,
		totalSlots,
		progressBar(nodeIndex, nodeSlots, 12),
		nodeIndex,
		nodeSlots,
		shortNodeName(source),
		shortNodeName(target),
		slot,
		phase,
	)
}

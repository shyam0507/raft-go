package internal

import "math/rand/v2"

func ElectionTimeout() int {
	return rand.IntN(300-150) + 150
}

// leader will send a hartbeat every HEART_BEAT_INTERVAL_LEADER if write operation is not happening
const HEART_BEAT_INTERVAL_LEADER = 100

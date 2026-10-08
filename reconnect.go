package celeris

import "time"

// retryDelay is full jitter: random × min(30s, 500ms × 2^retryIndex).
func retryDelay(retryIndex int, random func() float64) time.Duration {
	ceiling := retryDelayCap

	if retryIndex < 16 {
		ceiling = min(retryDelayCap, retryBaseDelay<<retryIndex)
	}

	return time.Duration(random() * float64(ceiling))
} // end function retryDelay

// replayLookback is the outage so far, rounded up to whole milliseconds, plus
// five seconds of overlap, capped at the server's largest lookback.
func replayLookback(outage time.Duration) time.Duration {
	lookback := outage.Truncate(time.Millisecond)

	if lookback < outage {
		lookback += time.Millisecond
	}

	return min(lookback+replayOverlap, replayLookbackCap)
} // end function replayLookback

package domain

import "time"

type JitterSource interface{ Int63n(int64) int64 }

func RetryDelay(attempt int, base, maximum time.Duration, jitter JitterSource) time.Duration {
	if attempt < 1 || base <= 0 || maximum <= 0 {
		return 0
	}
	delay := base
	for n := 1; n < attempt && delay < maximum; n++ {
		if delay > maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay > maximum {
		delay = maximum
	}
	if jitter == nil || delay >= maximum {
		return delay
	}
	room := maximum - delay
	capJitter := delay / 5
	if capJitter > room {
		capJitter = room
	}
	if capJitter <= 0 {
		return delay
	}
	return delay + time.Duration(jitter.Int63n(int64(capJitter)+1))
}

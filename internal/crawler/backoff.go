package crawler

import (
	"math"
	"math/rand"
	"time"
)

func backoffDuration(retryCount int) time.Duration {
	base := math.Pow(2, float64(retryCount))
	jitter := rand.Float64() * base * 0.5
	return time.Duration(base+jitter) * time.Second
}

package crawler

import (
	"math"
	"math/rand"
	"time"
)

const backoffJitterFactor = 0.5

func backoffDuration(retryCount int) time.Duration {
	base := math.Pow(2, float64(retryCount))
	jitter := rand.Float64() * base * backoffJitterFactor
	return time.Duration(base+jitter) * time.Second
}

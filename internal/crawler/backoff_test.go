package crawler

import (
	"testing"
	"time"
)

func TestBackoffDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		retryCount int
		minD       time.Duration
		maxD       time.Duration
	}{
		{
			name:       "retry 0: [1s, 1.5s)",
			retryCount: 0,
			minD:       1 * time.Second,
			maxD:       1500 * time.Millisecond,
		},
		{
			name:       "retry 1: [2s, 3s)",
			retryCount: 1,
			minD:       2 * time.Second,
			maxD:       3 * time.Second,
		},
		{
			name:       "retry 2: [4s, 6s)",
			retryCount: 2,
			minD:       4 * time.Second,
			maxD:       6 * time.Second,
		},
		{
			name:       "retry 3: [8s, 12s)",
			retryCount: 3,
			minD:       8 * time.Second,
			maxD:       12 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			for i := 0; i < 100; i++ {
				d := backoffDuration(tt.retryCount)
				if d < tt.minD || d >= tt.maxD {
					t.Errorf("backoffDuration(%d) = %v, want [%v, %v)", tt.retryCount, d, tt.minD, tt.maxD)
					break
				}
			}
		})
	}
}

func TestBackoffDuration_Increases(t *testing.T) {
	t.Parallel()
	// Run enough samples to verify that higher retries produce longer durations on average.
	var sum0, sum1, sum2 time.Duration
	n := 100
	for i := 0; i < n; i++ {
		sum0 += backoffDuration(0)
		sum1 += backoffDuration(1)
		sum2 += backoffDuration(2)
	}
	avg0 := sum0 / time.Duration(n)
	avg1 := sum1 / time.Duration(n)
	avg2 := sum2 / time.Duration(n)

	if avg1 <= avg0 {
		t.Errorf("avg retry 1 (%v) should be > avg retry 0 (%v)", avg1, avg0)
	}
	if avg2 <= avg1 {
		t.Errorf("avg retry 2 (%v) should be > avg retry 1 (%v)", avg2, avg1)
	}
}

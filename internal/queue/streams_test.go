package queue

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestIsBusyGroupError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"non-BUSYGROUP error", errors.New("some other error"), false},
		{"BUSYGROUP error", errors.New("BUSYGROUP Consumer Group name already exists"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isBusyGroupError(tt.err)
			if got != tt.want {
				t.Errorf("isBusyGroupError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestEnsureStreams_CreatesGroups(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	err := EnsureStreams(context.Background(), rdb, testLogger())
	if err != nil {
		t.Fatalf("EnsureStreams: %v", err)
	}

	// Verify frontier consumer group exists
	groups, err := rdb.XInfoGroups(context.Background(), FrontierStream).Result()
	if err != nil {
		t.Fatalf("XInfoGroups frontier: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != CrawlerGroup {
		t.Errorf("frontier groups = %v, want [%s]", groups, CrawlerGroup)
	}

	// Verify parse consumer group exists
	groups, err = rdb.XInfoGroups(context.Background(), ParseStream).Result()
	if err != nil {
		t.Fatalf("XInfoGroups parse: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != ParserGroup {
		t.Errorf("parse groups = %v, want [%s]", groups, ParserGroup)
	}
}

func TestEnsureStreams_Idempotent(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	err := EnsureStreams(context.Background(), rdb, testLogger())
	if err != nil {
		t.Fatalf("first EnsureStreams: %v", err)
	}

	err = EnsureStreams(context.Background(), rdb, testLogger())
	if err != nil {
		t.Fatalf("second EnsureStreams should be idempotent: %v", err)
	}
}

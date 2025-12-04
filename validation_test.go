package lockq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnqueueRepeat_ValidatesInterval tests that EnqueueRepeat rejects invalid intervals.
func TestEnqueueRepeat_ValidatesInterval(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()

	// Test zero interval
	_, err := q.EnqueueRepeat(ctx, "test_task", &TaskOptions{
		Payload: []byte("test"),
	}, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "repeat interval must be positive")

	// Test negative interval
	_, err = q.EnqueueRepeat(ctx, "test_task", &TaskOptions{
		Payload: []byte("test"),
	}, -5*time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "repeat interval must be positive")

	// Test positive interval (should succeed)
	_, err = q.EnqueueRepeat(ctx, "test_task", &TaskOptions{
		Payload: []byte("test"),
	}, 1*time.Second)
	require.NoError(t, err)
}

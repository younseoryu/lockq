package lockq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRepeatLockCleanup_OnDLQ tests that repeat locks are cleaned up when tasks move to DLQ.
func TestRepeatLockCleanup_OnDLQ(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		MaxRetries: 1, // Only 2 attempts total
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()
	lockKey := "user:cleanup-test"
	var attempts int32

	// Handler that always fails
	q.RegisterHandler("failing_repeat", func(ctx context.Context, payload []byte) error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("permanent failure")
	}, OnlyLocked())

	// Enqueue first repeat task
	taskID1, err := q.EnqueueRepeat(ctx, "failing_repeat", &TaskOptions{
		LockKey: lockKey,
		Payload: []byte("test1"),
	}, 100*time.Millisecond)
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for task to fail and move to DLQ
	time.Sleep(5 * time.Second)

	// Verify task in DLQ
	dlqCount, err := q.GetDeadLetterQueueCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dlqCount, "Failed task should be in DLQ")

	// Verify attempts
	finalAttempts := atomic.LoadInt32(&attempts)
	assert.Equal(t, int32(2), finalAttempts, "Task should have been attempted 2 times")

	// Now try to enqueue a NEW repeat task with the SAME lock key
	// This should succeed because repeat lock was cleaned up
	taskID2, err := q.EnqueueRepeat(ctx, "failing_repeat", &TaskOptions{
		LockKey: lockKey,
		Payload: []byte("test2"),
	}, 100*time.Millisecond)
	require.NoError(t, err, "Should be able to enqueue new repeat task after first one failed")
	assert.NotEqual(t, taskID1, taskID2, "New task should have different ID")

	// Clean up
	cancel()
	time.Sleep(100 * time.Millisecond)
	_ = q.DeleteTask(ctx, taskID1)
	_ = q.DeleteTask(ctx, taskID2)
}

// TestRepeatLockCleanup_OnSuccessfulDeletion verifies that DeleteTask cleans up repeat locks.
func TestRepeatLockCleanup_OnSuccessfulDeletion(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()
	lockKey := "user:delete-test"

	// Register handler
	q.RegisterHandler("test_task", func(ctx context.Context, payload []byte) error {
		return nil
	}, OnlyLocked())

	// Enqueue first repeat task
	taskID1, err := q.EnqueueRepeat(ctx, "test_task", &TaskOptions{
		LockKey: lockKey,
		Payload: []byte("test1"),
	}, 100*time.Millisecond)
	require.NoError(t, err)

	// Delete the task
	err = q.DeleteTask(ctx, taskID1)
	require.NoError(t, err)

	// Now enqueue another repeat task with same lock key - should succeed
	taskID2, err := q.EnqueueRepeat(ctx, "test_task", &TaskOptions{
		LockKey: lockKey,
		Payload: []byte("test2"),
	}, 100*time.Millisecond)
	require.NoError(t, err, "Should be able to enqueue new repeat task after deletion")
	assert.NotEqual(t, taskID1, taskID2, "New task should have different ID")

	_ = q.DeleteTask(ctx, taskID2)
}

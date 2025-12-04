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

// TestRetryLogic tests that failed tasks are retried up to MaxRetries.
func TestRetryLogic(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		MaxRetries: 3, // 1 initial + 3 retries = 4 total attempts
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()
	var attempts int32

	// Handler that fails first 3 times, succeeds on 4th
	q.RegisterHandler("test_task", func(ctx context.Context, payload []byte) error {
		current := atomic.AddInt32(&attempts, 1)
		if current < 4 {
			return errors.New("intentional failure")
		}
		return nil
	}, OnlyUnlocked())

	// Enqueue task
	taskID, err := q.Enqueue(ctx, "test_task", &TaskOptions{
		Payload: []byte("test"),
	})
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for retries to complete
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&attempts) >= 4 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// VERIFY: Task should have been attempted 4 times (1 initial + 3 retries)
	finalAttempts := atomic.LoadInt32(&attempts)
	assert.Equal(t, int32(4), finalAttempts, "Task should be attempted MaxRetries+1 times (Asynq convention)")

	// VERIFY: DLQ should be empty (task eventually succeeded)
	dlqCount, err := q.GetDeadLetterQueueCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), dlqCount, "DLQ should be empty for successful task")

	// Cleanup
	_ = q.DeleteTask(ctx, taskID)
}

// TestDeadLetterQueue tests that permanently failed tasks go to DLQ.
func TestDeadLetterQueue(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		MaxRetries: 2, // 1 initial + 2 retries = 3 total attempts
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()
	var attempts int32

	// Handler that always fails
	q.RegisterHandler("test_task", func(ctx context.Context, payload []byte) error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("permanent failure")
	}, OnlyUnlocked())

	// Enqueue task
	_, err = q.Enqueue(ctx, "test_task", &TaskOptions{
		Payload: []byte("test"),
	})
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for retries to exhaust
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		dlqCount, _ := q.GetDeadLetterQueueCount(ctx)
		if dlqCount > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// VERIFY: Task should have been attempted 3 times (1 initial + 2 retries)
	finalAttempts := atomic.LoadInt32(&attempts)
	assert.Equal(t, int32(3), finalAttempts, "Task should be attempted MaxRetries+1 times (Asynq convention)")

	// VERIFY: DLQ should contain the failed task
	dlqCount, err := q.GetDeadLetterQueueCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dlqCount, "Failed task should be in DLQ")
}

// TestObservabilityHooks tests that hooks are called correctly.
func TestObservabilityHooks(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()

	var startCalled int32
	var successCalled int32
	var failureCalled int32
	var retryCalled int32

	// Set up hooks
	q.SetTaskStartHook(func(task *Task) {
		atomic.AddInt32(&startCalled, 1)
	})

	q.SetTaskSuccessHook(func(task *Task, duration time.Duration) {
		atomic.AddInt32(&successCalled, 1)
	})

	q.SetTaskFailureHook(func(task *Task, err error, duration time.Duration) {
		atomic.AddInt32(&failureCalled, 1)
	})

	q.SetTaskRetryHook(func(task *Task, err error, duration time.Duration) {
		atomic.AddInt32(&retryCalled, 1)
	})

	// Register handler that succeeds
	q.RegisterHandler("success_task", func(ctx context.Context, payload []byte) error {
		return nil
	}, OnlyUnlocked())

	// Register handler that fails permanently
	var failAttempts int32
	q.RegisterHandler("fail_task", func(ctx context.Context, payload []byte) error {
		atomic.AddInt32(&failAttempts, 1)
		return errors.New("always fails")
	}, OnlyUnlocked())

	// Enqueue successful task
	_, err := q.Enqueue(ctx, "success_task", &TaskOptions{
		Payload: []byte("test"),
	})
	require.NoError(t, err)

	// Enqueue failing task
	_, err = q.Enqueue(ctx, "fail_task", &TaskOptions{
		Payload: []byte("test"),
	})
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for tasks to complete
	time.Sleep(15 * time.Second)

	// VERIFY: Hooks were called
	assert.GreaterOrEqual(t, atomic.LoadInt32(&startCalled), int32(2), "Start hook should be called")
	assert.GreaterOrEqual(t, atomic.LoadInt32(&successCalled), int32(1), "Success hook should be called")
	assert.GreaterOrEqual(t, atomic.LoadInt32(&retryCalled), int32(1), "Retry hook should be called")
	assert.GreaterOrEqual(t, atomic.LoadInt32(&failureCalled), int32(1), "Failure hook should be called")
}

// TestRepeatTaskDeletion tests that deleting a repeat task cleans up locks.
func TestRepeatTaskDeletion(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()
	lockKey := "user:123"

	// Register handler
	var execCount int32
	q.RegisterHandler("test_task", func(ctx context.Context, payload []byte) error {
		atomic.AddInt32(&execCount, 1)
		return nil
	}, OnlyLocked())

	// Enqueue repeat task
	taskID, err := q.EnqueueRepeat(ctx, "test_task", &TaskOptions{
		LockKey: lockKey,
		Payload: []byte("test"),
	}, 100*time.Millisecond)
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for first execution
	time.Sleep(50 * time.Millisecond)

	// VERIFY: Task executed at least once
	assert.GreaterOrEqual(t, atomic.LoadInt32(&execCount), int32(1), "Task should execute")

	// Delete the repeat task
	err = q.DeleteTask(ctx, taskID)
	require.NoError(t, err)

	// Try to enqueue another repeat task with same lock key
	// This should succeed now (proving the repeat lock was cleaned up)
	_, err = q.EnqueueRepeat(ctx, "test_task", &TaskOptions{
		LockKey: lockKey,
		Payload: []byte("test2"),
	}, 100*time.Millisecond)
	assert.NoError(t, err, "Should be able to enqueue new repeat task after deletion")
}

// TestContextCancellationDoesNotAffectHandlers tests that worker shutdown
// doesn't immediately cancel in-flight task contexts.
func TestContextCancellationDoesNotAffectHandlers(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()

	var handlerStarted int32
	var handlerCompleted int32

	// Register handler that takes some time
	q.RegisterHandler("test_task", func(ctx context.Context, payload []byte) error {
		atomic.StoreInt32(&handlerStarted, 1)

		// Simulate work
		select {
		case <-time.After(500 * time.Millisecond):
			atomic.StoreInt32(&handlerCompleted, 1)
			return nil
		case <-ctx.Done():
			// If context is canceled too early, this will trigger
			return ctx.Err()
		}
	}, OnlyUnlocked())

	// Enqueue task
	_, err := q.Enqueue(ctx, "test_task", &TaskOptions{
		Payload: []byte("test"),
	})
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	go func() { _ = q.Start(workerCtx) }()

	// Wait for handler to start
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&handlerStarted) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Cancel worker context while handler is running
	cancel()

	// Wait for handler to complete
	time.Sleep(600 * time.Millisecond)

	// VERIFY: Handler should have completed despite context cancellation
	assert.Equal(t, int32(1), atomic.LoadInt32(&handlerCompleted),
		"Handler should complete even when worker context is canceled")
}

// TestDoubleStopDoesNotPanic tests that calling Stop multiple times is safe.
func TestDoubleStopDoesNotPanic(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	go func() { _ = q.Start(workerCtx) }()

	time.Sleep(50 * time.Millisecond)

	// Stop via context cancel
	cancel()
	time.Sleep(50 * time.Millisecond)

	// Try to stop again via Stop method - should not panic
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()

	assert.NotPanics(t, func() {
		_ = q.Stop(stopCtx)
	}, "Calling Stop after context cancel should not panic")

	// Try to stop again - should not panic
	stopCtx2, stopCancel2 := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel2()

	assert.NotPanics(t, func() {
		_ = q.Stop(stopCtx2)
	}, "Calling Stop multiple times should not panic")
}

// TestGoroutineLeakOnShutdown tests that shutdown waits for in-flight tasks.
func TestGoroutineLeakOnShutdown(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()

	var taskStarted int32
	var taskCompleted int32

	// Register handler that takes time
	q.RegisterHandler("test_task", func(ctx context.Context, payload []byte) error {
		atomic.StoreInt32(&taskStarted, 1)
		time.Sleep(200 * time.Millisecond)
		atomic.StoreInt32(&taskCompleted, 1)
		return nil
	}, OnlyUnlocked())

	// Enqueue task
	_, err := q.Enqueue(ctx, "test_task", &TaskOptions{
		Payload: []byte("test"),
	})
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	go func() { _ = q.Start(workerCtx) }()

	// Wait for task to start
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&taskStarted) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Trigger shutdown while task is running
	cancel()

	// Wait a bit longer for proper shutdown
	time.Sleep(300 * time.Millisecond)

	// VERIFY: Task should have completed before shutdown
	assert.Equal(t, int32(1), atomic.LoadInt32(&taskCompleted),
		"In-flight task should complete before shutdown")
}

// TestJitterImplementation tests that jitter is properly random.
func TestJitterImplementation(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		MaxJitter: 100 * time.Millisecond,
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	// Get multiple jitter values
	jitters := make([]time.Duration, 10)
	for i := 0; i < 10; i++ {
		jitters[i] = q.getJitter()
	}

	// VERIFY: All jitters should be within bounds
	for i, jitter := range jitters {
		assert.GreaterOrEqual(t, jitter, time.Duration(0), "Jitter %d should be >= 0", i)
		assert.LessOrEqual(t, jitter, config.MaxJitter, "Jitter %d should be <= MaxJitter", i)
	}

	// VERIFY: Not all jitters should be the same (proves randomness)
	allSame := true
	for i := 1; i < len(jitters); i++ {
		if jitters[i] != jitters[0] {
			allSame = false
			break
		}
	}
	assert.False(t, allSame, "Jitter values should vary (not all the same)")
}

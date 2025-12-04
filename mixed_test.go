package lockq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMixed_SequentialExecution tests that tasks with same lock key execute sequentially
func TestMixed_SequentialExecution(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()
	lockKey := "user:123"

	var mu sync.Mutex
	executionOrder := make([]int, 0)
	executionTimes := make([]time.Time, 0)

	// Register handler that simulates work
	q.RegisterHandler("test_task", func(ctx context.Context, payload []byte) error {
		taskNum := int(payload[0])
		mu.Lock()
		executionOrder = append(executionOrder, taskNum)
		executionTimes = append(executionTimes, time.Now())
		mu.Unlock()
		// Simulate work
		time.Sleep(30 * time.Millisecond)
		return nil
	}, OnlyLocked())

	// Enqueue 3 tasks with same lock key (should execute sequentially)
	// With microsecond precision, insertion order is preserved
	for i := 1; i <= 3; i++ {
		_, err := q.Enqueue(ctx, "test_task", &TaskOptions{
			LockKey: lockKey,
			Payload: []byte{byte(i)},
		})
		require.NoError(t, err)
	}

	// DETERMINISTIC: Verify all 3 tasks are in queue
	count, err := q.Count(ctx, QueueLocked)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count, "Should have 3 tasks ready")

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for all tasks to execute
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(executionOrder) >= 3
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// DETERMINISTIC: Verify execution order (now deterministic with enqueue delays)
	mu.Lock()
	assert.Equal(t, []int{1, 2, 3}, executionOrder, "Tasks should execute in enqueue order")

	// DETERMINISTIC: Verify tasks executed sequentially (with gaps), NOT concurrently
	if len(executionTimes) >= 3 {
		gap1 := executionTimes[1].Sub(executionTimes[0])
		gap2 := executionTimes[2].Sub(executionTimes[1])

		// Each task takes 30ms, so gaps should be >= 25ms (allow 5ms tolerance)
		// This proves tasks ran sequentially, not concurrently
		assert.GreaterOrEqual(t, gap1, 25*time.Millisecond, "Tasks should execute sequentially, not concurrently")
		assert.GreaterOrEqual(t, gap2, 25*time.Millisecond, "Tasks should execute sequentially, not concurrently")
	}
	mu.Unlock()

	// DETERMINISTIC: Verify all tasks completed
	finalCount, err := q.CountAll(ctx, QueueLocked)
	require.NoError(t, err)
	assert.Equal(t, int64(0), finalCount, "All tasks should be processed")
}

// TestMixed_ConcurrentExecution tests that tasks with different lock keys execute concurrently
func TestMixed_ConcurrentExecution(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()

	var executing int32
	var maxConcurrent int32

	// Register handler that tracks concurrency
	q.RegisterHandler("test_task", func(ctx context.Context, payload []byte) error {
		current := atomic.AddInt32(&executing, 1)

		// Track max concurrent
		for {
			max := atomic.LoadInt32(&maxConcurrent)
			if current <= max || atomic.CompareAndSwapInt32(&maxConcurrent, max, current) {
				break
			}
		}

		// Simulate work
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&executing, -1)
		return nil
	}, OnlyLocked())

	// Enqueue 5 tasks with different lock keys (should execute concurrently)
	for i := 0; i < 5; i++ {
		lockKey := "user:" + string(rune('A'+i))
		_, err := q.Enqueue(ctx, "test_task", &TaskOptions{
			LockKey: lockKey,
			Payload: []byte{byte(i)},
		})
		require.NoError(t, err)
	}

	// DETERMINISTIC: Verify all tasks are in queue
	count, err := q.Count(ctx, QueueLocked)
	require.NoError(t, err)
	assert.Equal(t, int64(5), count, "Should have 5 tasks ready")

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for all tasks to complete
	time.Sleep(500 * time.Millisecond)

	// DETERMINISTIC: Verify multiple tasks ran concurrently
	maxConc := atomic.LoadInt32(&maxConcurrent)
	assert.Greater(t, maxConc, int32(1), "Tasks with different locks should run concurrently")

	t.Logf("Max concurrent executions: %d (expected > 1)", maxConc)

	// DETERMINISTIC: Verify all tasks completed
	finalCount, err := q.CountAll(ctx, QueueLocked)
	require.NoError(t, err)
	assert.Equal(t, int64(0), finalCount, "All tasks should be processed")
}

// TestMixed_RepeatAndOneOff tests mixing repeat and one-off tasks
func TestMixed_RepeatAndOneOff(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()

	var repeatCount int32
	var oneOffCount int32

	// Register handler
	q.RegisterHandler("test_task", func(ctx context.Context, payload []byte) error {
		if payload[0] == 'r' { // repeat-task
			atomic.AddInt32(&repeatCount, 1)
		} else {
			atomic.AddInt32(&oneOffCount, 1)
		}
		return nil
	}, LockedOrUnlocked())

	// Enqueue:
	// - 1 repeat task (should execute multiple times)
	// - 2 one-off tasks (should execute once each)

	interval := 150 * time.Millisecond
	repeatPayload := []byte("repeat-task")
	_, err := q.EnqueueRepeat(ctx, "test_task", &TaskOptions{
		LockKey: "user:123",
		Payload: repeatPayload,
	}, interval)
	require.NoError(t, err)

	oneOffPayload1 := []byte("oneoff-1")
	_, err = q.Enqueue(ctx, "test_task", &TaskOptions{
		LockKey: "user:456",
		Payload: oneOffPayload1,
	})
	require.NoError(t, err)

	oneOffPayload2 := []byte("oneoff-2")
	_, err = q.Enqueue(ctx, "test_task", &TaskOptions{
		Payload: oneOffPayload2,
	})
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for initial executions
	time.Sleep(100 * time.Millisecond)

	// DETERMINISTIC: Verify one-off tasks executed exactly once
	assert.Equal(t, int32(2), atomic.LoadInt32(&oneOffCount), "One-off tasks should execute once")

	// DETERMINISTIC: Verify repeat task executed at least once
	assert.GreaterOrEqual(t, atomic.LoadInt32(&repeatCount), int32(1), "Repeat task should execute")

	// Wait for repeat task to execute again
	time.Sleep(interval + 100*time.Millisecond)

	// DETERMINISTIC: Verify repeat task executed multiple times
	finalRepeatCount := atomic.LoadInt32(&repeatCount)
	assert.GreaterOrEqual(t, finalRepeatCount, int32(2), "Repeat task should execute multiple times")

	// DETERMINISTIC: Verify one-off count didn't change
	assert.Equal(t, int32(2), atomic.LoadInt32(&oneOffCount), "One-off tasks should not re-execute")
}

// TestMixed_DelayedAndImmediate tests mixing delayed and immediate tasks
func TestMixed_DelayedAndImmediate(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()

	var mu sync.Mutex
	executionOrder := make([]string, 0)
	executionTimes := make([]time.Time, 0)

	// Register handler
	q.RegisterHandler("test_task", func(ctx context.Context, payload []byte) error {
		mu.Lock()
		executionOrder = append(executionOrder, string(payload))
		executionTimes = append(executionTimes, time.Now())
		mu.Unlock()
		return nil
	}, OnlyUnlocked())

	// Enqueue immediate and delayed tasks
	immediatePayload := []byte("immediate")
	_, err := q.Enqueue(ctx, "test_task", &TaskOptions{
		Payload: immediatePayload,
	})
	require.NoError(t, err)

	delay := 150 * time.Millisecond
	delayedPayload := []byte("delayed")

	enqueueTime := time.Now()
	_, err = q.Enqueue(ctx, "test_task", &TaskOptions{
		Payload: delayedPayload,
		Delay:   delay,
	})
	require.NoError(t, err)

	// DETERMINISTIC: Verify immediate task is ready, delayed is not
	readyCount, err := q.Count(ctx, QueueUnlocked)
	require.NoError(t, err)
	assert.Equal(t, int64(1), readyCount, "Only immediate task should be ready")

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for both tasks to execute
	deadline := time.Now().Add(delay + 500*time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(executionOrder) >= 2
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// DETERMINISTIC: Verify execution order
	mu.Lock()
	assert.Len(t, executionOrder, 2, "Should have 2 executions")
	assert.Equal(t, string(immediatePayload), executionOrder[0], "Immediate task should execute first")
	assert.Equal(t, string(delayedPayload), executionOrder[1], "Delayed task should execute second")

	// DETERMINISTIC: Verify delayed task executed after delay (with tolerance)
	if len(executionTimes) >= 2 {
		actualDelay := executionTimes[1].Sub(enqueueTime)
		assert.GreaterOrEqual(t, actualDelay, delay-50*time.Millisecond, "Delayed task should execute after delay")
	}
	mu.Unlock()
}

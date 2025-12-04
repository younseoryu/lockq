package lockq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLockOwnership_ReleaseLockOnlyOwned verifies that ReleaseLock only releases
// locks owned by the calling task, preventing accidental release of another task's lock.
func TestLockOwnership_ReleaseLockOnlyOwned(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		LockTimeout: 30 * time.Second,
	}
	store, err := NewStore(rdb, config.WithDefaults())
	require.NoError(t, err)

	ctx := context.Background()
	lockKey := "user:123"
	lockRedisKey := config.WithDefaults().LockKeyPrefix() + lockKey

	// Task A acquires the lock
	taskA := &Task{
		ID:      "task-a-id",
		Type:    "test",
		LockKey: lockKey,
	}

	// Simulate Task A holding the lock
	err = rdb.Set(ctx, lockRedisKey, taskA.ID, 30*time.Second).Err()
	require.NoError(t, err)

	// Verify lock is held by Task A
	owner, err := rdb.Get(ctx, lockRedisKey).Result()
	require.NoError(t, err)
	assert.Equal(t, taskA.ID, owner)

	// Simulate lock expiry and Task B acquiring the lock
	err = rdb.Set(ctx, lockRedisKey, "task-b-id", 30*time.Second).Err()
	require.NoError(t, err)

	// Task A tries to release the lock (should fail silently - not release Task B's lock)
	err = store.ReleaseLock(ctx, taskA)
	require.NoError(t, err)

	// Verify Task B still owns the lock
	owner, err = rdb.Get(ctx, lockRedisKey).Result()
	require.NoError(t, err)
	assert.Equal(t, "task-b-id", owner, "Task B's lock should NOT be released by Task A")
}

// TestLockOwnership_ReleaseLockWhenOwned verifies that ReleaseLock works when the task owns the lock.
func TestLockOwnership_ReleaseLockWhenOwned(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		LockTimeout: 30 * time.Second,
	}
	store, err := NewStore(rdb, config.WithDefaults())
	require.NoError(t, err)

	ctx := context.Background()
	lockKey := "user:456"
	lockRedisKey := config.WithDefaults().LockKeyPrefix() + lockKey

	task := &Task{
		ID:      "task-owner-id",
		Type:    "test",
		LockKey: lockKey,
	}

	// Task acquires the lock
	err = rdb.Set(ctx, lockRedisKey, task.ID, 30*time.Second).Err()
	require.NoError(t, err)

	// Task releases its own lock (should succeed)
	err = store.ReleaseLock(ctx, task)
	require.NoError(t, err)

	// Verify lock is released
	exists, err := rdb.Exists(ctx, lockRedisKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists, "Lock should be released when task owns it")
}

// TestLockOwnership_UnlockOnlyOwned verifies that Unlock only releases locks owned by the task.
func TestLockOwnership_UnlockOnlyOwned(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		LockTimeout: 30 * time.Second,
	}
	store, err := NewStore(rdb, config.WithDefaults())
	require.NoError(t, err)

	ctx := context.Background()
	lockKey := "resource:789"
	lockRedisKey := config.WithDefaults().LockKeyPrefix() + lockKey
	taskKeyPrefix := config.WithDefaults().TaskKeyPrefix()

	// Create Task A (one-off task with lock)
	taskA := &Task{
		ID:      "task-a-unlock",
		Type:    "test",
		LockKey: lockKey,
	}

	// Store task data
	taskAKey := taskKeyPrefix + taskA.ID
	err = rdb.HSet(ctx, taskAKey, map[string]interface{}{
		"ty": taskA.Type,
		"lk": taskA.LockKey,
	}).Err()
	require.NoError(t, err)

	// Simulate Task A holding the lock initially, then Task B takes over
	err = rdb.Set(ctx, lockRedisKey, "task-b-unlock", 30*time.Second).Err()
	require.NoError(t, err)

	// Task A calls Unlock (should not release Task B's lock)
	err = store.Unlock(ctx, taskA)
	require.NoError(t, err)

	// Verify Task B still owns the lock
	owner, err := rdb.Get(ctx, lockRedisKey).Result()
	require.NoError(t, err)
	assert.Equal(t, "task-b-unlock", owner, "Task B's lock should NOT be released by Task A's Unlock")
}

// TestLockOwnership_ConcurrentTasksSameLockKey tests the race condition scenario:
// Task A times out, Task B acquires lock, Task A completes and tries to release.
func TestLockOwnership_ConcurrentTasksSameLockKey(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		LockTimeout: 100 * time.Millisecond, // Short timeout for test
	}
	store, err := NewStore(rdb, config.WithDefaults())
	require.NoError(t, err)

	ctx := context.Background()
	lockKey := "shared:resource"
	lockRedisKey := config.WithDefaults().LockKeyPrefix() + lockKey

	taskA := &Task{
		ID:      "task-concurrent-a",
		Type:    "slow_task",
		LockKey: lockKey,
	}

	taskB := &Task{
		ID:      "task-concurrent-b",
		Type:    "slow_task",
		LockKey: lockKey,
	}

	// Task A acquires lock
	err = rdb.Set(ctx, lockRedisKey, taskA.ID, config.LockTimeout).Err()
	require.NoError(t, err)

	// Simulate Task A taking too long - lock expires
	time.Sleep(150 * time.Millisecond)

	// Task B acquires the lock (Task A's expired)
	set, err := rdb.SetNX(ctx, lockRedisKey, taskB.ID, config.LockTimeout).Result()
	require.NoError(t, err)
	assert.True(t, set, "Task B should be able to acquire expired lock")

	// Task A finishes and tries to release (simulating error recovery)
	err = store.ReleaseLock(ctx, taskA)
	require.NoError(t, err)

	// CRITICAL: Task B's lock must still be held
	owner, err := rdb.Get(ctx, lockRedisKey).Result()
	require.NoError(t, err)
	assert.Equal(t, taskB.ID, owner, "Task B's lock must NOT be released by Task A")

	// Task B can now safely release its own lock
	err = store.ReleaseLock(ctx, taskB)
	require.NoError(t, err)

	exists, err := rdb.Exists(ctx, lockRedisKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists, "Lock should be released after Task B releases")
}

// TestLockOwnership_MoveToDLQReleasesOwnLock verifies that MoveToDeadLetter
// only releases the lock if the task owns it.
func TestLockOwnership_MoveToDLQReleasesOwnLock(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		LockTimeout: 30 * time.Second,
	}
	store, err := NewStore(rdb, config.WithDefaults())
	require.NoError(t, err)

	ctx := context.Background()
	lockKey := "dlq:test"
	lockRedisKey := config.WithDefaults().LockKeyPrefix() + lockKey
	taskKeyPrefix := config.WithDefaults().TaskKeyPrefix()

	// Task that will fail
	task := &Task{
		ID:      "task-dlq-test",
		Type:    "failing_task",
		LockKey: lockKey,
		Repeat:  0, // One-off task
	}

	// Store task data
	taskKey := taskKeyPrefix + task.ID
	err = rdb.HSet(ctx, taskKey, map[string]interface{}{
		"ty": task.Type,
		"lk": task.LockKey,
	}).Err()
	require.NoError(t, err)

	// Simulate another task holding the lock (lock was stolen/expired)
	err = rdb.Set(ctx, lockRedisKey, "other-task-id", 30*time.Second).Err()
	require.NoError(t, err)

	// Move task to DLQ (should NOT release the other task's lock)
	err = store.MoveToDeadLetter(ctx, task, assert.AnError)
	require.NoError(t, err)

	// Verify the other task's lock is still held
	owner, err := rdb.Get(ctx, lockRedisKey).Result()
	require.NoError(t, err)
	assert.Equal(t, "other-task-id", owner, "Other task's lock should NOT be released")
}

// TestLockOwnership_MoveToDLQReleasesWhenOwned verifies MoveToDeadLetter releases lock when owned.
func TestLockOwnership_MoveToDLQReleasesWhenOwned(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		LockTimeout: 30 * time.Second,
	}
	store, err := NewStore(rdb, config.WithDefaults())
	require.NoError(t, err)

	ctx := context.Background()
	lockKey := "dlq:owned"
	lockRedisKey := config.WithDefaults().LockKeyPrefix() + lockKey
	taskKeyPrefix := config.WithDefaults().TaskKeyPrefix()

	task := &Task{
		ID:      "task-dlq-owned",
		Type:    "failing_task",
		LockKey: lockKey,
		Repeat:  0,
	}

	// Store task data
	taskKey := taskKeyPrefix + task.ID
	err = rdb.HSet(ctx, taskKey, map[string]interface{}{
		"ty": task.Type,
		"lk": task.LockKey,
	}).Err()
	require.NoError(t, err)

	// Task owns the lock
	err = rdb.Set(ctx, lockRedisKey, task.ID, 30*time.Second).Err()
	require.NoError(t, err)

	// Move task to DLQ (should release its own lock)
	err = store.MoveToDeadLetter(ctx, task, assert.AnError)
	require.NoError(t, err)

	// Verify lock is released
	exists, err := rdb.Exists(ctx, lockRedisKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists, "Task's own lock should be released when moving to DLQ")
}

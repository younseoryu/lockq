package lockq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

// ErrTaskExists is returned when a locked repeating task with the same lock key already exists.
var ErrTaskExists = errors.New("identical repeating task type with same lock key already exists")

// Store handles Redis operations for task persistence and retrieval.
type Store struct {
	redis  *redis.Client
	config *Config

	pushUnlockedSHA  string
	pushLockedSHA    string
	popUnlockedSHA   string
	popLockedSHA     string
	unlockSHA        string
	countSHA         string
	resetAttemptsSHA string
	releaseLockSHA   string
	moveToDLQSHA     string
	deleteTaskSHA    string
	retryFromDLQSHA  string
	requeueSHA       string
}

// NewStore creates a Store instance and preloads Lua scripts into Redis.
func NewStore(rdb *redis.Client, config *Config) (*Store, error) {
	config = config.WithDefaults()

	s := &Store{
		redis:  rdb,
		config: config,
	}

	ctx := context.Background()
	var err error

	s.pushUnlockedSHA, err = rdb.ScriptLoad(ctx, pushUnlocked).Result()
	if err != nil {
		return nil, fmt.Errorf("load push unlocked script: %w", err)
	}

	s.pushLockedSHA, err = rdb.ScriptLoad(ctx, pushLocked).Result()
	if err != nil {
		return nil, fmt.Errorf("load push locked script: %w", err)
	}

	s.popUnlockedSHA, err = rdb.ScriptLoad(ctx, popUnlocked).Result()
	if err != nil {
		return nil, fmt.Errorf("load pop unlocked script: %w", err)
	}

	s.popLockedSHA, err = rdb.ScriptLoad(ctx, popLocked).Result()
	if err != nil {
		return nil, fmt.Errorf("load pop locked script: %w", err)
	}

	s.unlockSHA, err = rdb.ScriptLoad(ctx, unlock).Result()
	if err != nil {
		return nil, fmt.Errorf("load unlock script: %w", err)
	}

	s.countSHA, err = rdb.ScriptLoad(ctx, count).Result()
	if err != nil {
		return nil, fmt.Errorf("load count script: %w", err)
	}

	s.resetAttemptsSHA, err = rdb.ScriptLoad(ctx, resetAttempts).Result()
	if err != nil {
		return nil, fmt.Errorf("load reset attempts script: %w", err)
	}

	s.releaseLockSHA, err = rdb.ScriptLoad(ctx, releaseLock).Result()
	if err != nil {
		return nil, fmt.Errorf("load release lock script: %w", err)
	}

	s.moveToDLQSHA, err = rdb.ScriptLoad(ctx, moveToDLQ).Result()
	if err != nil {
		return nil, fmt.Errorf("load move to dlq script: %w", err)
	}

	s.deleteTaskSHA, err = rdb.ScriptLoad(ctx, deleteTask).Result()
	if err != nil {
		return nil, fmt.Errorf("load delete task script: %w", err)
	}

	s.retryFromDLQSHA, err = rdb.ScriptLoad(ctx, retryFromDLQ).Result()
	if err != nil {
		return nil, fmt.Errorf("load retry from dlq script: %w", err)
	}

	s.requeueSHA, err = rdb.ScriptLoad(ctx, requeue).Result()
	if err != nil {
		return nil, fmt.Errorf("load requeue script: %w", err)
	}

	return s, nil
}

// Push adds a task to the appropriate queue based on its lock key.
func (s *Store) Push(ctx context.Context, task *Task) (string, error) {
	if task.ID == "" {
		task.ID = uuid.NewString()
	}

	delay := time.Until(task.ScheduledAt)
	if delay < 0 {
		delay = 0
	}

	if task.IsLocked() {
		return s.pushLocked(ctx, task, delay)
	}
	return s.pushUnlocked(ctx, task, delay)
}

func (s *Store) pushUnlocked(ctx context.Context, task *Task, delay time.Duration) (string, error) {
	queueKey := s.config.UnlockedQueueKeyFor(task.Type)

	result, err := s.redis.EvalSha(
		ctx,
		s.pushUnlockedSHA,
		[]string{queueKey},
		task.ID,
		task.Type,
		task.Payload,
		delay.Milliseconds(),
		task.Repeat.Milliseconds(),
		s.config.TaskKeyPrefix(),
		task.MaxAttempts,
	).Result()

	if err != nil {
		return "", fmt.Errorf("eval push unlocked: %w", err)
	}

	return result.(string), nil
}

func (s *Store) pushLocked(ctx context.Context, task *Task, delay time.Duration) (string, error) {
	queueKey := s.config.LockedQueueKeyFor(task.Type)

	result, err := s.redis.EvalSha(
		ctx,
		s.pushLockedSHA,
		[]string{queueKey},
		task.ID,
		task.Type,
		task.Payload,
		delay.Milliseconds(),
		task.LockKey,
		task.Repeat.Milliseconds(),
		s.config.TaskKeyPrefix(),
		s.config.RepeatLockKeyPrefix(),
		task.MaxAttempts,
	).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", ErrTaskExists
		}
		return "", fmt.Errorf("eval push locked: %w", err)
	}

	return result.(string), nil
}

// PopUnlocked retrieves and locks a task from the unlocked queue for the given task type.
func (s *Store) PopUnlocked(ctx context.Context, taskType string) (*Task, error) {
	queueKey := s.config.UnlockedQueueKeyFor(taskType)

	result, err := s.redis.EvalSha(
		ctx,
		s.popUnlockedSHA,
		[]string{queueKey},
		s.config.TaskKeyPrefix(),
		s.config.LockTimeout.Milliseconds(),
	).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) || err.Error() == "redis: nil" {
			return nil, nil
		}
		return nil, fmt.Errorf("eval pop unlocked: %w", err)
	}

	return s.parseTaskResult(result)
}

// PopLocked retrieves and locks a task from the locked queue for the given task type
// if its lock is available.
func (s *Store) PopLocked(ctx context.Context, taskType string) (*Task, error) {
	queueKey := s.config.LockedQueueKeyFor(taskType)

	result, err := s.redis.EvalSha(
		ctx,
		s.popLockedSHA,
		[]string{queueKey},
		s.config.PeekSize,
		s.config.FallbackPeekSize,
		s.config.LockKeyPrefix(),
		s.config.LockTimeout.Milliseconds(),
		s.config.TaskKeyPrefix(),
	).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) || err.Error() == "redis: nil" {
			return nil, nil
		}
		return nil, fmt.Errorf("eval pop locked: %w", err)
	}

	return s.parseTaskResult(result)
}

func (s *Store) parseTaskResult(result interface{}) (*Task, error) {
	data, ok := result.([]interface{})
	if !ok || len(data) < 8 {
		return nil, fmt.Errorf("invalid task result format")
	}

	task := &Task{
		ID:          toString(data[0]),
		Type:        toString(data[1]),
		Payload:     []byte(toString(data[2])),
		LockKey:     toString(data[3]),
		Attempts:    int(toInt64(data[5])),
		MaxAttempts: int(toInt64(data[6])),
		ExecutionID: toString(data[7]),
	}

	if repeatMs := toInt64(data[4]); repeatMs > 0 {
		task.Repeat = time.Duration(repeatMs) * time.Millisecond
	}

	return task, nil
}

// Unlock cleans up task state after processing.
func (s *Store) Unlock(ctx context.Context, task *Task) error {
	taskKey := s.config.TaskKeyPrefix() + task.ID

	_, err := s.redis.EvalSha(
		ctx,
		s.unlockSHA,
		[]string{taskKey},
		s.config.LockKeyPrefix(),
		s.config.RepeatLockKeyPrefix(),
		task.ID,
		task.ExecutionID,
	).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) || err.Error() == "redis: nil" {
			return nil
		}
		return fmt.Errorf("eval unlock: %w", err)
	}

	return nil
}

// ReleaseLock releases a distributed lock if owned by the current execution.
func (s *Store) ReleaseLock(ctx context.Context, task *Task) error {
	if task.LockKey == "" {
		return nil
	}

	lockKey := s.config.LockKeyPrefix() + task.LockKey

	_, err := s.redis.EvalSha(
		ctx,
		s.releaseLockSHA,
		[]string{lockKey},
		task.ExecutionID,
	).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) || err.Error() == "redis: nil" {
			return nil
		}
		return fmt.Errorf("eval release lock: %w", err)
	}

	return nil
}

// Requeue returns a task to its queue for retry after the specified delay.
func (s *Store) Requeue(ctx context.Context, task *Task, delay time.Duration) error {
	var queueKey string
	if task.IsLocked() {
		queueKey = s.config.LockedQueueKeyFor(task.Type)
	} else {
		queueKey = s.config.UnlockedQueueKeyFor(task.Type)
	}

	taskKey := s.config.TaskKeyPrefix() + task.ID

	scoreUs := time.Now().Add(delay).UnixMicro()

	_, err := s.redis.EvalSha(
		ctx,
		s.requeueSHA,
		[]string{queueKey, taskKey},
		scoreUs,
		task.ID,
	).Result()
	if err != nil {
		return fmt.Errorf("requeue task: %w", err)
	}

	return nil
}

// MoveToDeadLetter moves a failed task to the dead letter queue.
func (s *Store) MoveToDeadLetter(ctx context.Context, task *Task, taskErr error) error {
	taskKey := s.config.TaskKeyPrefix() + task.ID

	var queueKey string
	if task.IsLocked() {
		queueKey = s.config.LockedQueueKeyFor(task.Type)
	} else {
		queueKey = s.config.UnlockedQueueKeyFor(task.Type)
	}

	isRepeat := "0"
	if task.IsRepeat() {
		isRepeat = "1"
	}
	isLocked := "0"
	if task.IsLocked() {
		isLocked = "1"
	}

	_, err := s.redis.EvalSha(
		ctx,
		s.moveToDLQSHA,
		[]string{taskKey},
		task.ID,
		queueKey,
		s.config.DeadLetterQueueKey(),
		s.config.LockKeyPrefix(),
		s.config.RepeatLockKeyPrefix(),
		taskErr.Error(),
		time.Now().Unix(),
		isRepeat,
		isLocked,
		task.ExecutionID,
	).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) || err.Error() == "redis: nil" {
			return nil
		}
		return fmt.Errorf("eval move to dlq: %w", err)
	}

	return nil
}

// DeleteTask removes a task and its associated locks from Redis.
func (s *Store) DeleteTask(ctx context.Context, taskID string) error {
	taskKey := s.config.TaskKeyPrefix() + taskID

	ty, err := s.redis.HGet(ctx, taskKey, taskFieldType).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || err.Error() == "redis: nil" {
			return nil
		}
		return fmt.Errorf("lookup task type for delete: %w", err)
	}

	lockedQueueKey := s.config.LockedQueueKeyFor(ty)
	unlockedQueueKey := s.config.UnlockedQueueKeyFor(ty)

	_, err = s.redis.EvalSha(
		ctx,
		s.deleteTaskSHA,
		[]string{taskKey},
		taskID,
		lockedQueueKey,
		unlockedQueueKey,
		s.config.DeadLetterQueueKey(),
		s.config.LockKeyPrefix(),
		s.config.RepeatLockKeyPrefix(),
	).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) || err.Error() == "redis: nil" {
			return nil
		}
		return fmt.Errorf("eval delete task: %w", err)
	}

	return nil
}

// Count returns the number of tasks ready within timeDelta from now.
func (s *Store) Count(ctx context.Context, queueKey string, timeDelta time.Duration) (int64, error) {
	result, err := s.redis.EvalSha(
		ctx,
		s.countSHA,
		[]string{queueKey},
		timeDelta.Milliseconds(),
	).Result()

	if err != nil {
		return 0, fmt.Errorf("eval count: %w", err)
	}

	return result.(int64), nil
}

// CountAll returns the total number of tasks in the queue regardless of scheduled time.
func (s *Store) CountAll(ctx context.Context, queueKey string) (int64, error) {
	return s.redis.ZCard(ctx, queueKey).Result()
}

// RetryFromDLQ moves a task from the dead letter queue back to processing.
func (s *Store) RetryFromDLQ(ctx context.Context, taskID string) error {
	taskKey := s.config.TaskKeyPrefix() + taskID

	ty, err := s.redis.HGet(ctx, taskKey, taskFieldType).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || err.Error() == "redis: nil" {
			return fmt.Errorf("task %s not found", taskID)
		}
		return fmt.Errorf("lookup task type for retry from dlq: %w", err)
	}

	lockedQueueKey := s.config.LockedQueueKeyFor(ty)
	unlockedQueueKey := s.config.UnlockedQueueKeyFor(ty)

	result, err := s.redis.EvalSha(
		ctx,
		s.retryFromDLQSHA,
		[]string{taskKey},
		taskID,
		s.config.DeadLetterQueueKey(),
		lockedQueueKey,
		unlockedQueueKey,
		s.config.RepeatLockKeyPrefix(),
	).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) || err.Error() == "redis: nil" {
			return fmt.Errorf("task %s not found", taskID)
		}
		return fmt.Errorf("eval retry from dlq: %w", err)
	}

	if m, ok := result.(map[interface{}]interface{}); ok {
		if errMsg, exists := m["err"]; exists {
			return fmt.Errorf("%v", errMsg)
		}
	}

	return nil
}

// ResetAttempts resets the attempts counter if executionID matches.
func (s *Store) ResetAttempts(ctx context.Context, taskID string, executionID string) (bool, error) {
	taskKey := s.config.TaskKeyPrefix() + taskID

	result, err := s.redis.EvalSha(
		ctx,
		s.resetAttemptsSHA,
		[]string{taskKey},
		executionID,
	).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, fmt.Errorf("eval reset attempts: %w", err)
	}

	if result == false {
		return false, nil
	}

	return true, nil
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func toInt64(v interface{}) int64 {
	if i, ok := v.(int64); ok {
		return i
	}
	return 0
}

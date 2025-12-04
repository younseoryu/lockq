package lockq

import (
	"context"
	"time"
)

// TaskOptions specifies optional parameters for enqueueing a task.
type TaskOptions struct {
	// Payload is the task data.
	Payload []byte
	// LockKey enables serial execution. Empty string means no locking.
	LockKey string
	// Delay specifies how long to wait before the task becomes ready.
	Delay time.Duration
}

// Task represents a unit of work to be processed by the queue.
type Task struct {
	ID          string
	Type        string
	Payload     []byte
	LockKey     string
	Repeat      time.Duration
	Attempts    int
	MaxAttempts int
	ExecutionID string // Unique ID for this specific execution, used to verify ownership
	CreatedAt   time.Time
	ScheduledAt time.Time
}

// IsLocked returns true if the task requires distributed locking.
func (t *Task) IsLocked() bool {
	return t.LockKey != ""
}

// IsRepeat returns true if the task should repeat at a fixed interval.
func (t *Task) IsRepeat() bool {
	return t.Repeat > 0
}

// NextRunTime returns when the task should execute next.
func (t *Task) NextRunTime() time.Time {
	if !t.IsRepeat() {
		return t.ScheduledAt
	}
	return time.Now().Add(t.Repeat)
}

// HandlerFunc processes a task payload and returns an error if processing fails.
type HandlerFunc func(ctx context.Context, payload []byte) error

// TaskHook is called when a task starts.
type TaskHook func(task *Task)

// TaskResultHook is called when a task completes (success or failure).
type TaskResultHook func(task *Task, duration time.Duration)

// TaskErrorHook is called when a task fails or is retried.
type TaskErrorHook func(task *Task, err error, duration time.Duration)

// QueueType distinguishes between locked and unlocked queues.
type QueueType int

const (
	// QueueUnlocked is for tasks that don't require distributed locking.
	QueueUnlocked QueueType = iota
	// QueueLocked is for tasks that require distributed locking.
	QueueLocked
)

// QueueType returns which queue type this task belongs to.
func (t *Task) QueueType() QueueType {
	if t.IsLocked() {
		return QueueLocked
	}
	return QueueUnlocked
}

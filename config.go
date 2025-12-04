package lockq

import "time"

// Config contains queue configuration parameters.
type Config struct {
	// UnlockedQueue is the name of the unlocked queue.
	UnlockedQueue string
	// LockedQueue is the name of the locked queue.
	LockedQueue string
	// LockTimeout is the maximum duration a task can be locked.
	LockTimeout time.Duration
	// MaxRetries is the number of retry attempts before moving to DLQ.
	MaxRetries int
	// InitialBackoff is the starting backoff duration when queue is empty.
	InitialBackoff time.Duration
	// MaxBackoff is the maximum backoff duration.
	MaxBackoff time.Duration
	// MaxJitter adds randomness to backoff to prevent thundering herd.
	MaxJitter time.Duration
	// ErrorDelay is the wait time after an error before retrying.
	ErrorDelay time.Duration
	// PeekSize is the initial number of tasks to scan for available locks.
	PeekSize int
	// FallbackPeekSize is the extended scan size if initial peek finds nothing.
	FallbackPeekSize int
	// KeyPrefix is prepended to all Redis keys.
	KeyPrefix string
}

// WithDefaults applies default values to unset config fields.
func (c *Config) WithDefaults() *Config {
	if c == nil {
		c = &Config{}
	}

	if c.UnlockedQueue == "" {
		c.UnlockedQueue = "default"
	}
	if c.LockedQueue == "" {
		c.LockedQueue = "default"
	}
	if c.LockTimeout == 0 {
		c.LockTimeout = 30 * time.Second
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}

	if c.InitialBackoff == 0 {
		c.InitialBackoff = 25 * time.Millisecond
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = 500 * time.Millisecond
	}
	if c.MaxJitter == 0 {
		c.MaxJitter = 25 * time.Millisecond
	}
	if c.ErrorDelay == 0 {
		c.ErrorDelay = 1 * time.Second
	}

	if c.PeekSize == 0 {
		c.PeekSize = 25
	}
	if c.FallbackPeekSize == 0 {
		c.FallbackPeekSize = 100
	}

	if c.KeyPrefix == "" {
		c.KeyPrefix = "lockq:"
	}

	return c
}

// UnlockedQueueKeyFor returns the Redis key for the unlocked queue of taskType.
func (c *Config) UnlockedQueueKeyFor(taskType string) string {
	if taskType == "" {
		return c.KeyPrefix + "q:unlocked:" + c.UnlockedQueue
	}
	return c.KeyPrefix + "q:unlocked:" + c.UnlockedQueue + ":" + taskType
}

// LockedQueueKeyFor returns the Redis key for the locked queue of taskType.
func (c *Config) LockedQueueKeyFor(taskType string) string {
	if taskType == "" {
		return c.KeyPrefix + "q:locked:" + c.LockedQueue
	}
	return c.KeyPrefix + "q:locked:" + c.LockedQueue + ":" + taskType
}

// TaskKeyPrefix returns the Redis key prefix for task data.
func (c *Config) TaskKeyPrefix() string {
	return c.KeyPrefix + "t:"
}

// LockKeyPrefix returns the Redis key prefix for distributed locks.
func (c *Config) LockKeyPrefix() string {
	return c.KeyPrefix + "l:"
}

// RepeatLockKeyPrefix returns the Redis key prefix for repeating task locks.
func (c *Config) RepeatLockKeyPrefix() string {
	return c.KeyPrefix + "rl:"
}

// DeadLetterQueueKey returns the Redis key for the dead letter queue.
func (c *Config) DeadLetterQueueKey() string {
	return c.KeyPrefix + "dlq"
}

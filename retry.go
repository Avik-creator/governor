package governor

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// ResourceRetry is the quota every retry, at any layer, draws one unit from.
const ResourceRetry = "retry"

// retryPolicy is how Retry waits and when it gives up by itself.
type retryPolicy struct {
	attempts int // most attempts in total; zero means only the budget bounds them
	base     time.Duration
	max      time.Duration
}

// RetryOption adjusts one Retry.
type RetryOption func(*retryPolicy)

// WithMaxAttempts stops after n attempts in total, even if budget remains.
func WithMaxAttempts(n int) RetryOption {
	return func(p *retryPolicy) { p.attempts = n }
}

// WithBackoff waits base before the first retry, doubling up to max.
func WithBackoff(base, max time.Duration) RetryOption {
	return func(p *retryPolicy) { p.base, p.max = base, max }
}

// final reports whether err is a verdict of Governor, which no retry can change.
func final(err error) bool {
	for _, verdict := range []error{
		ErrDenied, ErrTaskEnded, ErrSessionLost, ErrClientClosed, ErrNoTask, ErrForbidden,
		context.Canceled, context.DeadlineExceeded,
	} {
		if errors.Is(err, verdict) {
			return true
		}
	}
	return false
}

// Retry runs fn until it succeeds; the first attempt is free and each retry costs one retry unit.
func Retry(ctx context.Context, fn func(context.Context) error, opts ...RetryOption) error {
	policy := retryPolicy{base: 100 * time.Millisecond, max: 5 * time.Second}
	for _, opt := range opts {
		opt(&policy)
	}
	wait := policy.base
	for attempt := 1; ; attempt++ {
		err := fn(ctx)
		if err == nil || final(err) || attempt == policy.attempts {
			return err
		}
		// The budget is shared by the whole tree, so nested retries cannot multiply.
		if budgetErr := Consume(ctx, ResourceRetry, 1); budgetErr != nil {
			return errors.Join(err, budgetErr)
		}
		// Full jitter keeps workers that failed together from retrying together.
		select {
		case <-ctx.Done():
			return errors.Join(err, context.Cause(ctx))
		case <-time.After(rand.N(wait) + 1):
		}
		wait = min(2*wait, policy.max)
	}
}

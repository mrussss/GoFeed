package feed

import (
	"context"
	"errors"
	"time"
)

const (
	feedReadConcurrency = 8
	feedReadWait        = 20 * time.Millisecond
	feedQueryTimeout    = 250 * time.Millisecond
)

var ErrFeedReadOverloaded = errors.New("feed read overloaded")

type feedReadBudget struct {
	active  chan struct{}
	waiting chan struct{}
}

func newFeedReadBudget() feedReadBudget {
	return feedReadBudget{
		active:  make(chan struct{}, feedReadConcurrency),
		waiting: make(chan struct{}, feedReadConcurrency),
	}
}

func (budget feedReadBudget) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case budget.active <- struct{}{}:
		return nil
	default:
	}
	select {
	case budget.waiting <- struct{}{}:
		defer func() { <-budget.waiting }()
	default:
		return ErrFeedReadOverloaded
	}

	timer := time.NewTimer(feedReadWait)
	defer timer.Stop()
	select {
	case budget.active <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		if err := ctx.Err(); err != nil {
			return err
		}
		return ErrFeedReadOverloaded
	}
}

// readFeedFallbackMySQL reserves one slot for a fallback or rebuild query.
func readFeedFallbackMySQL[T any](ctx context.Context, budget feedReadBudget, query func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := budget.acquire(ctx); err != nil {
		return zero, err
	}
	defer func() { <-budget.active }()
	return readFeedMySQL(ctx, query)
}

// readFeedMySQL bounds connection pool wait and execution for one query.
func readFeedMySQL[T any](ctx context.Context, query func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, feedQueryTimeout)
	defer cancel()
	return query(queryCtx)
}

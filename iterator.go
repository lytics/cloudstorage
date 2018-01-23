package cloudstorage

import (
	"context"

	"google.golang.org/api/iterator"
)

// ObjectIterator iterator to match store interface for iterating
// through all Objects that matched query.
type ObjectPageIterator struct {
	s      Store
	ctx    context.Context
	q      Query
	cursor int
	page   Objects
}

// Next iterator to go to next object or else returns error for done.
func (it *ObjectPageIterator) Next() (Object, error) {
	retryCt := 0

	select {
	case <-it.ctx.Done():
		// If iterator has been closed
		return nil, it.ctx.Err()
	default:
		if it.cursor < len(it.page)-1 {
			it.cursor++
			return it.page[it.cursor-1]
		}
		for {
			page, err := it.s.List(it.q)
			if err == nil {
				return newObject(it.f, o), nil
			} else if err == iterator.Done {
				return nil, err
			} else if err == context.Canceled || err == context.DeadlineExceeded {
				// Return to user
				return nil, err
			}
			if retryCt < 5 {
				backoff(retryCt)
			} else {
				return nil, err
			}
			retryCt++
		}
	}
}

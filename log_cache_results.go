package gosnowflake

import (
	"context"
)

func logResultCacheComparisonFromContext(ctx context.Context) bool {
	val := ctx.Value(logResultCacheComparison)
	if val == nil {
		return false
	}
	a, ok := val.(bool)
	return a && ok
}

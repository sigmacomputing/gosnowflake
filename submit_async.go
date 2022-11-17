package gosnowflake

// this is a sigma only file
// when rebasing you can ignore everthing in this file as long as this
// file makes it into master

import (
	"context"
	"database/sql/driver"
	"strconv"
)

type isComplete bool
type queryID string

func (sc *snowflakeConn) SubmitAsync(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (queryID, isComplete, error) {
	ctx = WithAsyncMode(WithAsyncModeNoFetch(ctx))
	qid, err := getResumeQueryID(ctx)
	if err != nil {
		return "", false, err
	}

	// if query already submitted, return the query id
	if qid != "" {
		return queryID(qid), false, nil
	}

	// if the query is not submitted, submit it
	logger.WithContext(ctx).Infof("Query: %#v, %v", query, args)

	if sc.rest == nil {
		return "", false, driver.ErrBadConn
	}

	ctx = setResultType(ctx, queryResultType)
	isDesc := isDescribeOnly(ctx)

	data, err := sc.exec(ctx, query, true /* noResult */, false /* isInternal */, isDesc, args)
	if err != nil {
		logger.WithContext(ctx).Errorf("error: %v", err)
		if data != nil {
			code, err := strconv.Atoi(data.Code)
			if err != nil {
				return "", true, err
			}
			return "", true, (&SnowflakeError{
				Number:   code,
				SQLState: data.Data.SQLState,
				Message:  err.Error(),
				QueryID:  data.Data.QueryID,
			}).exceptionTelemetry(sc)
		}
		return "", true, err
	}

	qid = data.Data.QueryID
	_, err = sc.checkQueryStatus(ctx, qid)
	if err == nil {
		// no error means query is complete
		return queryID(qid), true, nil
	}

	return queryID(qid), false, nil
}

package gosnowflake

// this is a sigma only file
// when rebasing you can ignore everthing in this file as long as this
// file makes it into master

import (
	"context"
	"database/sql/driver"
	"strconv"
)

// SubmitAsyncResponse is the return type for submitAsync. QueryId and boolian value to indicate whether we have to come back and
// wait for a query to complete.
type SubmitAsyncResponse struct {
	QueryID  string
	Complete bool
}

// SubmitAsync returns a QueryID which can be used to check the status of a query (or later, get query results when a
// query is completed) and returns whether the query is complete so we can avoid coming back to wait for it to complete
func (sc *snowflakeConn) SubmitAsync(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (SubmitAsyncResponse, error) {
	ctx = WithAsyncMode(WithAsyncModeNoFetch(ctx))
	qid, err := getResumeQueryID(ctx)
	if err != nil {
		return SubmitAsyncResponse{
			QueryID:  "",
			Complete: false,
		}, err
	}

	// if query already submitted, return the query id
	if qid != "" {
		return SubmitAsyncResponse{
			QueryID:  qid,
			Complete: sc.checkIfComplete(ctx, qid),
		}, err
	}

	// if the query is not submitted, submit it
	logger.WithContext(ctx).Infof("Query: %#v, %v", query, args)

	if sc.rest == nil {
		return SubmitAsyncResponse{
			QueryID:  "",
			Complete: false,
		}, err
	}

	ctx = setResultType(ctx, queryResultType)
	isDesc := isDescribeOnly(ctx)

	data, err := sc.exec(ctx, query, true /* noResult */, false /* isInternal */, isDesc, args)
	if err != nil {
		logger.WithContext(ctx).Errorf("error: %v", err)
		if data != nil {
			code, err := strconv.Atoi(data.Code)
			if err != nil {
				return SubmitAsyncResponse{
					QueryID:  qid,
					Complete: true,
				}, err
			}
			return SubmitAsyncResponse{
					QueryID:  qid,
					Complete: true,
				}, (&SnowflakeError{
					Number:   code,
					SQLState: data.Data.SQLState,
					Message:  err.Error(),
					QueryID:  data.Data.QueryID,
				}).exceptionTelemetry(sc)
		}
		return SubmitAsyncResponse{
			QueryID:  qid,
			Complete: true,
		}, err
	}

	qid = data.Data.QueryID

	// after the query is submitted, we check the status of the query. This will save us time because the entire round trip of checking
	// status from here only takes 100 ms, so for fast running queries this will prevent another roundtrip from multiplex, and also prevent
	// inconsistencies that can occur for very fast running queries
	return SubmitAsyncResponse{
		QueryID:  qid,
		Complete: sc.checkIfComplete(ctx, qid),
	}, nil
}

func (sc *snowflakeConn) checkIfComplete(ctx context.Context, qid string) bool {
	_, err := sc.checkQueryStatus(ctx, qid)
	if err == nil || err.(*SnowflakeError).Number != ErrQueryIsRunning {
		// no error means query is complete; error that isnt running means query complete but failed
		return true
	}
	return false
}

// AsyncSubmitter is an interface which allows a query to be submitted
// and then allows us to wait for the query to complete given the query id
// this interface is needed for multiplex recovery
//
// The raw gosnowflake connection implements this interface and we
// export it so that clients can access this functionality, bypassing
// the alternative which is using the queryContext built into dbSql
type AsyncSubmitter interface {
	SubmitAsync(context.Context, string, []driver.NamedValue) (SubmitAsyncResponse, error)
}

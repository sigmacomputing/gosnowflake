// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
)

type paramKey string

const (
	// MultiStatementCount controls the number of queries to execute in a single API call
	MultiStatementCount paramKey = "MULTI_STATEMENT_COUNT"
)

type snowflakeStmt struct {
	sc    *snowflakeConn
	query string
}

// Close TODO
func (stmt *SnowflakeStmt) Close() error {
	glog.V(2).Infoln("Stmt.Close")
	// noop
	return nil
}

// NumInput TODO
func (stmt *SnowflakeStmt) NumInput() int {
	glog.V(2).Infoln("Stmt.NumInput")
	// Go Snowflake doesn't know the number of binding parameters.
	return -1
}

// DescribeContext TODO
// NOTE this function isn't actually part of any of the `database/sql` interfaces. As such the SnowflakeStmt
// struct must be public so that calling code can typecast and call the function.
func (stmt *SnowflakeStmt) DescribeContext(ctx context.Context, args ...driver.NamedValue) ([]ColumnType, error) {
	glog.V(2).Infoln("Stmt.DescribeContext")
	return stmt.sc.DescribeContext(ctx, stmt.query, args)
}

// ExecContext TODO
func (stmt *SnowflakeStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	glog.V(2).Infoln("Stmt.ExecContext")
	return stmt.sc.ExecContext(ctx, stmt.query, args)
}

// QueryContext TODO
func (stmt *SnowflakeStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	glog.V(2).Infoln("Stmt.QueryContext")
	return stmt.sc.QueryContext(ctx, stmt.query, args)
}

// Exec TODO
func (stmt *SnowflakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	glog.V(2).Infoln("Stmt.Exec")
	return stmt.sc.Exec(stmt.query, args)
}

// Query TODO
func (stmt *SnowflakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	glog.V(2).Infoln("Stmt.Query")
	return stmt.sc.Query(stmt.query, args)
}

// WithMultiStatement returns a context that allows the user to execute the desired number of sql queries in one query
func WithMultiStatement(ctx context.Context, num int) (context.Context, error) {
	return context.WithValue(ctx, MultiStatementCount, num), nil
}

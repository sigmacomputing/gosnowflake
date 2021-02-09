// Copyright (c) 2020-2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
)

func TestGetQueryID(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	ctx := context.TODO()
	conn, _ := db.Conn(ctx)

	err = conn.Raw(func(x interface{}) error {
		stmt, err := x.(driver.ConnPrepareContext).PrepareContext(ctx, "select 1")
		if err != nil {
			return err
		}
		rows, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
		if err != nil {
			return err
		}
		defer rows.Close()
		qid := rows.(SnowflakeResult).GetQueryID()
		if qid == "" {
			t.Fatal("should have returned a query ID string")
		}

		_, err = x.(driver.ConnPrepareContext).PrepareContext(ctx, "selectt 1")
		if err == nil {
			t.Fatal("should have failed to execute query")
		}
		if driverErr, ok := err.(*SnowflakeError); ok {
			if driverErr.Number != 1003 {
				t.Fatalf("incorrect error code. expected: 1003, got: %v", driverErr.Number)
			}
			if driverErr.QueryID == "" {
				t.Fatal("should have an associated query ID")
			}
		} else {
			t.Fatal("should have been able to cast to Snowflake Error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to prepare statement. err: %v", err)
	}
}

func TestEmitQueryID(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	queryIDChan := make(chan string, 1)
	numrows := 100000
	ctx, _ := WithAsyncMode(context.Background())
	ctx = WithQueryIDChan(ctx, queryIDChan)

	goRoutineChan := make(chan string)
	go func(grCh chan string, qIDch chan string) {
		queryID := <-queryIDChan
		grCh <- queryID
	}(goRoutineChan, queryIDChan)

	cnt := 0
	var idx int
	var v string
	rows, _ := db.QueryContext(ctx, fmt.Sprintf("SELECT SEQ8(), RANDSTR(1000, RANDOM()) FROM TABLE(GENERATOR(ROWCOUNT=>%v))", numrows))
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&idx, &v)
		if err != nil {
			t.Fatal(err)
		}
		cnt++
	}
	logger.Infof("NextResultSet: %v", rows.NextResultSet())

	queryID := <-goRoutineChan
	if queryID == "" {
		t.Fatal("expected a nonempty query ID")
	}
	if cnt != numrows {
		t.Errorf("number of rows didn't match. expected: %v, got: %v", numrows, cnt)
	}
}

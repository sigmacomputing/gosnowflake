// Copyright (c) 2017-2018 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	statementTypeIDDml              = int64(0x3000)
	statementTypeIDInsert           = statementTypeIDDml + int64(0x100)
	statementTypeIDUpdate           = statementTypeIDDml + int64(0x200)
	statementTypeIDDelete           = statementTypeIDDml + int64(0x300)
	statementTypeIDMerge            = statementTypeIDDml + int64(0x400)
	statementTypeIDMultiTableInsert = statementTypeIDDml + int64(0x500)
)

const (
	sessionClientSessionKeepAlive = "client_session_keep_alive"
)

type queryTagKeyType int

const (
	_                                       = iota
	queryTagKey       queryTagKeyType       = iota
)

type QueryTag struct {
	RequestId string  `json:"requestId"`
	Email string      `json:"email"`
}

var (
	// FetchQueryMonitoringDataThresholdMs specifies the ms threshold, over which we'll fetch the monitoring
	// data for a Snowflake query. We use a time-based threshold, since there is a non-zero latency cost
	// to fetch this data and we want to bound the additional latency. By default we bound to a 2% increase
	// in latency - assuming worst case 100ms - when fetching this metadata.
	FetchQueryMonitoringDataThreshold time.Duration = 5 * time.Second
)

type snowflakeConn struct {
	cfg            *Config
	rest           *snowflakeRestful
	SequeceCounter uint64
	QueryID        string
	SQLState       string
}

// isDml returns true if the statement type code is in the range of DML.
func (sc *snowflakeConn) isDml(v int64) bool {
	switch v {
	case statementTypeIDDml, statementTypeIDInsert,
		statementTypeIDUpdate, statementTypeIDDelete,
		statementTypeIDMerge, statementTypeIDMultiTableInsert:
		return true
	}
	return false
}

func (sc *snowflakeConn) exec(
	ctx context.Context,
	query string,
	noResult bool,
	isInternal bool,
	describeOnly bool,
	parameters []driver.NamedValue,
) (*execResponse, error) {
	var err error
	counter := atomic.AddUint64(&sc.SequeceCounter, 1) // query sequence counter

	req := execRequest{
		SQLText:      query,
		AsyncExec:    noResult,
		SequenceID:   counter,
		DescribeOnly: describeOnly,
      Parameters: make(map[string]string),
	}
	req.IsInternal = isInternal
    queryTag := sc.GetContextQueryTag(ctx)
    if queryTag != nil {
        jsonQueryTag, err := json.Marshal(queryTag)
        if err != nil {
            return nil, err
        }
        req.Parameters["QUERY_TAG"] = string(jsonQueryTag)
    }
	tsmode := "TIMESTAMP_NTZ"
	idx := 1
	if len(parameters) > 0 {
		req.Bindings = make(map[string]execBindParameter, len(parameters))
		for i, n := 0, len(parameters); i < n; i++ {
			t := goTypeToSnowflake(parameters[i].Value, tsmode)
			glog.V(2).Infof("tmode: %v\n", t)
			if t == "CHANGE_TYPE" {
				tsmode, err = dataTypeMode(parameters[i].Value)
				if err != nil {
					return nil, err
				}
			} else {
				v1, err := valueToString(parameters[i].Value, tsmode)
				if err != nil {
					return nil, err
				}
				req.Bindings[strconv.Itoa(idx)] = execBindParameter{
					Type:  t,
					Value: v1,
				}
				idx++
			}
		}
	}
	glog.V(2).Infof("bindings: %v", req.Bindings)

	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeApplicationSnowflake // TODO v1.1: change to JSON in case of PUT/GET
	headers["User-Agent"] = userAgent

	jsonBody, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	var data *execResponse
	data, err = sc.rest.FuncPostQuery(ctx, sc.rest, &url.Values{}, headers, jsonBody, sc.rest.RequestTimeout)
	if err != nil {
		return nil, err
	}
	var code int
	if data.Code != "" {
		code, err = strconv.Atoi(data.Code)
		if err != nil {
			code = -1
			return nil, err
		}
	} else {
		code = -1
	}
	glog.V(2).Infof("Success: %v, Code: %v", data.Success, code)
	if !data.Success {
		return nil, &SnowflakeError{
			Number:   code,
			SQLState: data.Data.SQLState,
			Message:  data.Message,
			QueryID:  data.Data.QueryID,
		}
	}
	glog.V(2).Info("Exec/Query SUCCESS")
	sc.cfg.Database = data.Data.FinalDatabaseName
	sc.cfg.Schema = data.Data.FinalSchemaName
	sc.cfg.Role = data.Data.FinalRoleName
	sc.cfg.Warehouse = data.Data.FinalWarehouseName
	sc.QueryID = data.Data.QueryID
	sc.SQLState = data.Data.SQLState
	sc.populateSessionParameters(data.Data.Parameters)
	return data, err
}

func (sc *snowflakeConn) monitoring(qid string, runtimeSec time.Duration) (*QueryMonitoringData, error) {
	// Exit early if this was a "fast" query
	if runtimeSec < FetchQueryMonitoringDataThreshold {
		return nil, nil
	}

	fullURL := fmt.Sprintf(
		"%s://%s:%d/monitoring/queries/%s",
		sc.rest.Protocol, sc.rest.Host, sc.rest.Port, qid,
	)

	// Bound the GET request to 1 second in the absolute worst case.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	headers := make(map[string]string)
	headers["accept"] = "application/json"
	headers["User-Agent"] = userAgent
	headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sc.rest.Token)

	resp, err := sc.rest.FuncGet(ctx, sc.rest, fullURL, headers, sc.rest.RequestTimeout)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// (max) NOTE we don't expect this to fail
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("response returned code %d: %v", resp.StatusCode, b)
	}

	var m monitoringResponse
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}

	if len(m.Data.Queries) != 1 {
		return nil, nil
	}

	return &m.Data.Queries[0], nil
}

func (sc *snowflakeConn) Begin() (driver.Tx, error) {
	return sc.BeginTx(context.TODO(), driver.TxOptions{})
}

func (sc *snowflakeConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	glog.V(2).Info("BeginTx")
	if opts.ReadOnly {
		return nil, &SnowflakeError{
			Number:   ErrNoReadOnlyTransaction,
			SQLState: SQLStateFeatureNotSupported,
			Message:  errMsgNoReadOnlyTransaction,
		}
	}
	if int(opts.Isolation) != int(sql.LevelDefault) {
		return nil, &SnowflakeError{
			Number:   ErrNoDefaultTransactionIsolationLevel,
			SQLState: SQLStateFeatureNotSupported,
			Message:  errMsgNoDefaultTransactionIsolationLevel,
		}
	}
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	_, err := sc.exec(ctx, "BEGIN", false, false, false, nil)
	if err != nil {
		return nil, err
	}
	return &snowflakeTx{sc}, err
}

func (sc *snowflakeConn) cleanup() {
	glog.Flush() // must flush log buffer while the process is running.
	sc.rest = nil
	sc.cfg = nil
}

func (sc *snowflakeConn) Close() (err error) {
	glog.V(2).Infoln("Close")
	sc.stopHeartBeat()

	// ensure transaction is rollbacked
	ctx := context.TODO()
	_, err = sc.exec(ctx, "ROLLBACK", false, false, false, nil)
	if err != nil {
		glog.V(2).Info(err)
	}

	err = sc.rest.FuncCloseSession(ctx, sc.rest, sc.rest.RequestTimeout)
	if err != nil {
		glog.V(2).Info(err)
	}
	sc.cleanup()
	return nil
}

func (sc *snowflakeConn) WithContextQueryTag(ctx context.Context, qt QueryTag) context.Context {
	glog.V(2).Infoln("Adding QUERY_TAG")
	return context.WithValue(ctx, queryTagKey, qt)
}

func (sc *snowflakeConn) GetContextQueryTag(ctx context.Context) *QueryTag {
	glog.V(2).Infoln("Retrieving QUERY_TAG")
	if qt, ok := ctx.Value(queryTagKey).(*QueryTag); ok {
		return qt
	}
	return nil
}

func (sc *snowflakeConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	glog.V(2).Infoln("Prepare")
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	stmt := &SnowflakeStmt{
		sc:    sc,
		query: query,
	}
	return stmt, nil
}

func (sc *snowflakeConn) Prepare(query string) (driver.Stmt, error) {
	return sc.PrepareContext(context.TODO(), query)
}

func (sc *snowflakeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	glog.V(2).Infof("Exec: %#v, %v", query, args)
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	// TODO: handle noResult and isInternal
	qStart := time.Now()
	data, err := sc.exec(ctx, query, false, false, false, args)
	if err != nil {
		return nil, err
	}
	var updatedRows int64
	if sc.isDml(data.Data.StatementTypeID) {
		// collects all values from the returned row sets
		updatedRows = 0
		for i, n := 0, len(data.Data.RowType); i < n; i++ {
			v, err := strconv.ParseInt(*data.Data.RowSet[0][i], 10, 64)
			if err != nil {
				return nil, err
			}
			updatedRows += v
		}
		glog.V(2).Infof("number of updated rows: %#v", updatedRows)
		rows := &snowflakeResult{
			affectedRows: updatedRows,
			insertID:     -1,
			queryID:      sc.QueryID,
		}

		if m, err := sc.monitoring(sc.QueryID, time.Since(qStart)); err == nil {
			rows.monitoring = m
		}
		return rows, nil
	}
	glog.V(2).Info("DDL")
	return driver.ResultNoRows, nil
}

func (sc *snowflakeConn) DescribeContext(ctx context.Context, query string, args []driver.NamedValue) ([]ColumnType, error) {
	glog.V(2).Infof("Describe: %#v, %v", query, args)
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	// TODO: handle noResult and isInternal
	data, err := sc.exec(ctx, query, false, false, true, args)
	if err != nil {
		glog.V(2).Infof("error: %v", err)
		return nil, err
	}

	return rowTypesToColumnTypes(data.Data.RowType), err
}

func (sc *snowflakeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	glog.V(2).Infof("Query: %#v, %v", query, args)
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	// TODO: handle noResult and isInternal
	qStart := time.Now()
	data, err := sc.exec(ctx, query, false, false, false, args)
	if err != nil {
		glog.V(2).Infof("error: %v", err)
		return nil, err
	}

	rows := new(snowflakeRows)
	rows.sc = sc
	rows.RowType = data.Data.RowType
	rows.ChunkDownloader = &snowflakeChunkDownloader{
		sc:                 sc,
		ctx:                ctx,
		CurrentChunk:       data.Data.RowSet,
		ChunkMetas:         data.Data.Chunks,
		Total:              int64(data.Data.Total),
		TotalRowIndex:      int64(-1),
		CellCount:          len(data.Data.RowType),
		Qrmk:               data.Data.Qrmk,
		ChunkHeader:        data.Data.ChunkHeaders,
		FuncDownload:       downloadChunk,
		FuncDownloadHelper: downloadChunkHelper,
		FuncGet:            getChunk,
	}

	if m, err := sc.monitoring(sc.QueryID, time.Since(qStart)); err == nil {
		rows.monitoring = m
	}

	rows.queryID = sc.QueryID
	rows.ChunkDownloader.start()
	return rows, nil
}

func (sc *snowflakeConn) Exec(
	query string,
	args []driver.Value) (
	driver.Result, error) {
	return sc.ExecContext(context.TODO(), query, toNamedValues(args))
}

func (sc *snowflakeConn) Query(
	query string,
	args []driver.Value) (
	driver.Rows, error) {
	return sc.QueryContext(context.TODO(), query, toNamedValues(args))
}

func (sc *snowflakeConn) Ping(ctx context.Context) error {
	glog.V(2).Infoln("Ping")
	if sc.rest == nil {
		return driver.ErrBadConn
	}
	// TODO: handle noResult and isInternal
	_, err := sc.exec(ctx, "SELECT 1", false, false, false, []driver.NamedValue{})
	return err
}

func (sc *snowflakeConn) populateSessionParameters(parameters []nameValueParameter) {
	// other session parameters (not all)
	glog.V(2).Infof("params: %#v", parameters)
	for _, param := range parameters {
		v := ""
		switch param.Value.(type) {
		case int64:
			if vv, ok := param.Value.(int64); ok {
				v = strconv.FormatInt(vv, 10)
			}
		case float64:
			if vv, ok := param.Value.(float64); ok {
				v = strconv.FormatFloat(vv, 'g', -1, 64)
			}
		case bool:
			if vv, ok := param.Value.(bool); ok {
				v = strconv.FormatBool(vv)
			}
		default:
			if vv, ok := param.Value.(string); ok {
				v = vv
			}
		}
		glog.V(3).Infof("parameter. name: %v, value: %v", param.Name, v)
		sc.cfg.Params[strings.ToLower(param.Name)] = &v
	}
}

func (sc *snowflakeConn) isClientSessionKeepAliveEnabled() bool {
	v, ok := sc.cfg.Params[sessionClientSessionKeepAlive]
	if !ok {
		return false
	}
	return strings.Compare(*v, "true") == 0
}

func (sc *snowflakeConn) startHeartBeat() {
	if !sc.isClientSessionKeepAliveEnabled() {
		return
	}
	sc.rest.HeartBeat = &heartbeat{
		restful: sc.rest,
	}
	sc.rest.HeartBeat.start()
}

func (sc *snowflakeConn) stopHeartBeat() {
	if !sc.isClientSessionKeepAliveEnabled() {
		return
	}
	sc.rest.HeartBeat.stop()
}

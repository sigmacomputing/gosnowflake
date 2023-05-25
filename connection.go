// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v12/arrow/ipc"
)

const (
	httpHeaderContentType      = "Content-Type"
	httpHeaderAccept           = "accept"
	httpHeaderUserAgent        = "User-Agent"
	httpHeaderServiceName      = "X-Snowflake-Service"
	httpHeaderContentLength    = "Content-Length"
	httpHeaderHost             = "Host"
	httpHeaderValueOctetStream = "application/octet-stream"
	httpHeaderContentEncoding  = "Content-Encoding"
)

const (
	statementTypeIDMulti            = int64(0x1000)
	statementTypeIDDml              = int64(0x3000)
	statementTypeIDMultiTableInsert = statementTypeIDDml + int64(0x500)
)

const (
	sessionClientSessionKeepAlive          = "client_session_keep_alive"
	sessionClientValidateDefaultParameters = "CLIENT_VALIDATE_DEFAULT_PARAMETERS"
	sessionArrayBindStageThreshold         = "client_stage_array_binding_threshold"
	serviceName                            = "service_name"
)

type resultType string

const (
	snowflakeResultType contextKey = "snowflakeResultType"
	execResultType      resultType = "exec"
	queryResultType     resultType = "query"
)

const privateLinkSuffix = "privatelink.snowflakecomputing.com"

type snowflakeConn struct {
	ctx             context.Context
	cfg             *Config
	rest            *snowflakeRestful
	restMu          sync.RWMutex // guard shutdown race
	SequenceCounter uint64
	QueryID         string
	SQLState        string
	telemetry       *snowflakeTelemetry
	internal        InternalClient
	execRespCache   *execRespCache
}

var (
	queryIDPattern = `[\w\-_]+`
	queryIDRegexp  = regexp.MustCompile(queryIDPattern)
)

func (sc *snowflakeConn) exec(
	ctx context.Context,
	query string,
	noResult bool,
	isInternal bool,
	describeOnly bool,
	bindings []driver.NamedValue) (
	*execResponse, error) {
	var err error
	counter := atomic.AddUint64(&sc.SequenceCounter, 1) // query sequence counter

	req := execRequest{
		SQLText:      query,
		AsyncExec:    noResult,
		Parameters:   map[string]interface{}{},
		IsInternal:   isInternal,
		DescribeOnly: describeOnly,
		SequenceID:   counter,
	}
	if key := ctx.Value(multiStatementCount); key != nil {
		req.Parameters[string(multiStatementCount)] = key
	}
	if tag := ctx.Value(queryTag); tag != nil {
		req.Parameters[string(queryTag)] = tag
	}
	logger.WithContext(ctx).Infof("parameters: %v", req.Parameters)

	// handle bindings, if required
	requestID := getOrGenerateRequestIDFromContext(ctx)
	if len(bindings) > 0 {
		if err = sc.processBindings(ctx, bindings, describeOnly, requestID, &req); err != nil {
			return nil, err
		}
	}
	logger.WithContext(ctx).Infof("bindings: %v", req.Bindings)

	// populate headers
	headers := getHeaders()
	if isFileTransfer(query) {
		headers[httpHeaderAccept] = headerContentTypeApplicationJSON
	}
	paramsMutex.Lock()
	if serviceName, ok := sc.cfg.Params[serviceName]; ok {
		headers[httpHeaderServiceName] = *serviceName
	}
	paramsMutex.Unlock()

	jsonBody, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	data, err := sc.rest.FuncPostQuery(ctx, sc.rest, &url.Values{}, headers,
		jsonBody, sc.rest.RequestTimeout, requestID, sc.cfg)
	if err != nil {
		return data, err
	}
	code := -1
	if data.Code != "" {
		code, err = strconv.Atoi(data.Code)
		if err != nil {
			return data, err
		}
	}
	logger.WithContext(ctx).Infof("Success: %v, Code: %v", data.Success, code)
	if !data.Success {
		return nil, (populateErrorFields(code, data)).exceptionTelemetry(sc)
	}

	// handle PUT/GET commands
	if isFileTransfer(query) {
		data, err = sc.processFileTransfer(ctx, data, query, isInternal)
		if err != nil {
			return nil, err
		}
	}

	logger.WithContext(ctx).Info("Exec/Query SUCCESS")
	sc.cfg.Database = data.Data.FinalDatabaseName
	sc.cfg.Schema = data.Data.FinalSchemaName
	sc.cfg.Role = data.Data.FinalRoleName
	sc.cfg.Warehouse = data.Data.FinalWarehouseName
	sc.QueryID = data.Data.QueryID
	sc.SQLState = data.Data.SQLState
	sc.populateSessionParameters(data.Data.Parameters)
	return data, err
}

func (sc *snowflakeConn) Begin() (driver.Tx, error) {
	return sc.BeginTx(sc.ctx, driver.TxOptions{})
}

func (sc *snowflakeConn) BeginTx(
	ctx context.Context,
	opts driver.TxOptions) (
	driver.Tx, error) {
	logger.WithContext(ctx).Info("BeginTx")
	if opts.ReadOnly {
		return nil, (&SnowflakeError{
			Number:   ErrNoReadOnlyTransaction,
			SQLState: SQLStateFeatureNotSupported,
			Message:  errMsgNoReadOnlyTransaction,
		}).exceptionTelemetry(sc)
	}
	if int(opts.Isolation) != int(sql.LevelDefault) {
		return nil, (&SnowflakeError{
			Number:   ErrNoDefaultTransactionIsolationLevel,
			SQLState: SQLStateFeatureNotSupported,
			Message:  errMsgNoDefaultTransactionIsolationLevel,
		}).exceptionTelemetry(sc)
	}
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	isDesc := isDescribeOnly(ctx)
	if _, err := sc.exec(ctx, "BEGIN", false, /* noResult */
		false /* isInternal */, isDesc, nil); err != nil {
		return nil, err
	}
	return &snowflakeTx{sc}, nil
}

func (sc *snowflakeConn) cleanup() {
	// must flush log buffer while the process is running.
	if sc.rest != nil && sc.rest.Client != nil {
		sc.rest.Client.CloseIdleConnections()
	}
	sc.restMu.Lock()
	defer sc.restMu.Unlock()
	sc.rest = nil
	sc.cfg = nil

	releaseExecRespCache(sc.execRespCache)
	sc.execRespCache = nil
}

func (sc *snowflakeConn) Close() (err error) {
	logger.WithContext(sc.ctx).Infoln("Close")
	sc.telemetry.sendBatch()
	sc.stopHeartBeat()
	defer sc.cleanup()

	if !sc.cfg.KeepSessionAlive {
		if err = sc.rest.FuncCloseSession(sc.ctx, sc.rest, sc.rest.RequestTimeout); err != nil {
			logger.Error(err)
		}
	}
	return nil
}

func (sc *snowflakeConn) PrepareContext(
	ctx context.Context,
	query string) (
	driver.Stmt, error) {
	logger.WithContext(sc.ctx).Infoln("Prepare")
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	stmt := &snowflakeStmt{
		sc:    sc,
		query: query,
	}
	return stmt, nil
}

func (sc *snowflakeConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue) (
	driver.Result, error) {
	logger.WithContext(ctx).Infof("Exec: %#v, %v", query, args)
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	noResult := isAsyncMode(ctx)
	isDesc := isDescribeOnly(ctx)
	// TODO handle isInternal
	ctx = setResultType(ctx, execResultType)
	qStart := time.Now()
	data, err := sc.exec(ctx, query, noResult, false /* isInternal */, isDesc, args)
	if err != nil {
		logger.WithContext(ctx).Infof("error: %v", err)
		if data != nil {
			code, err := strconv.Atoi(data.Code)
			if err != nil {
				return nil, err
			}
			return nil, (&SnowflakeError{
				Number:   code,
				SQLState: data.Data.SQLState,
				Message:  err.Error(),
				QueryID:  data.Data.QueryID,
			}).exceptionTelemetry(sc)
		}
		return nil, err
	}

	// if async exec, return result object right away
	if noResult {
		return data.Data.AsyncResult, nil
	}

	if isDml(data.Data.StatementTypeID) {
		// collects all values from the returned row sets
		updatedRows, err := updateRows(data.Data)
		if err != nil {
			return nil, err
		}
		logger.WithContext(ctx).Debugf("number of updated rows: %#v", updatedRows)
		rows := &snowflakeResult{
			affectedRows: updatedRows,
			insertID:     -1,
			queryID:      sc.QueryID,
		} // last insert id is not supported by Snowflake

		rows.monitoring = mkMonitoringFetcher(sc, sc.QueryID, time.Since(qStart))

		return rows, nil
	} else if isMultiStmt(&data.Data) {
		rows, err := sc.handleMultiExec(ctx, data.Data)
		if err != nil {
			return nil, err
		}
		rows.monitoring = mkMonitoringFetcher(sc, sc.QueryID, time.Since(qStart))

		return rows, nil
	}
	logger.Debug("DDL")
	return driver.ResultNoRows, nil
}

func (sc *snowflakeConn) QueryContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue) (
	driver.Rows, error) {
	qid, err := getResumeQueryID(ctx)
	if err != nil {
		return nil, err
	}
	if qid == "" {
		return sc.queryContextInternal(ctx, query, args)
	}

	// check the query status to find out if there is a result to fetch
	_, err = sc.checkQueryStatus(ctx, qid)
	snowflakeErr, isSnowflakeError := err.(*SnowflakeError)
	if err == nil || (isSnowflakeError && snowflakeErr.Number == ErrQueryIsRunning) {
		// the query is running. Rows object will be returned from here.
		return sc.buildRowsForRunningQuery(ctx, qid)
	}
	return nil, err
}

func (sc *snowflakeConn) queryContextInternal(
	ctx context.Context,
	query string,
	args []driver.NamedValue) (
	driver.Rows, error) {
	logger.WithContext(ctx).Infof("Query: %#v, %v", query, args)
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}

	noResult := isAsyncMode(ctx)
	isDesc := isDescribeOnly(ctx)
	ctx = setResultType(ctx, queryResultType)
	qStart := time.Now()
	// TODO: handle isInternal
	data, err := sc.exec(ctx, query, noResult, false /* isInternal */, isDesc, args)
	if err != nil {
		logger.WithContext(ctx).Errorf("error: %v", err)
		if data != nil {
			code, err := strconv.Atoi(data.Code)
			if err != nil {
				return nil, err
			}
			return nil, (&SnowflakeError{
				Number:   code,
				SQLState: data.Data.SQLState,
				Message:  err.Error(),
				QueryID:  data.Data.QueryID,
			}).exceptionTelemetry(sc)
		}
		return nil, err
	}

	// if async query, return row object right away
	if noResult {
		return data.Data.AsyncRows, nil
	}

	rows := new(snowflakeRows)
	rows.sc = sc
	rows.queryID = sc.QueryID
	rows.monitoring = mkMonitoringFetcher(sc, sc.QueryID, time.Since(qStart))

	if isSubmitSync(ctx) && data.Code == queryInProgressCode {
		rows.status = QueryStatusInProgress
		return rows, nil
	}
	rows.status = QueryStatusComplete

	if isMultiStmt(&data.Data) {
		// handleMultiQuery is responsible to fill rows with childResults
		if err = sc.handleMultiQuery(ctx, data.Data, rows); err != nil {
			return nil, err
		}
		if data.Data.ResultIDs == "" && rows.ChunkDownloader == nil {
			// SIG-16907: We have no results to download here.
			logger.WithContext(ctx).Errorf("Encountered empty result-ids for a multi-statement request. Query-id: %s, Query: %s", data.Data.QueryID, query)
			return nil, (&SnowflakeError{
				Number:   ErrQueryIDFormat,
				SQLState: data.Data.SQLState,
				Message:  "ExecResponse for multi-statement request had no ResultIDs",
				QueryID:  data.Data.QueryID,
			}).exceptionTelemetry(sc)
		}
	} else {
		rows.addDownloader(populateChunkDownloader(ctx, sc, data.Data))
	}

	if startErr := rows.ChunkDownloader.start(); startErr != nil {
		return nil, startErr
	}
	return rows, err
}

func (sc *snowflakeConn) Prepare(query string) (driver.Stmt, error) {
	return sc.PrepareContext(sc.ctx, query)
}

func (sc *snowflakeConn) Exec(
	query string,
	args []driver.Value) (
	driver.Result, error) {
	return sc.ExecContext(sc.ctx, query, toNamedValues(args))
}

func (sc *snowflakeConn) Query(
	query string,
	args []driver.Value) (
	driver.Rows, error) {
	return sc.QueryContext(sc.ctx, query, toNamedValues(args))
}

func (sc *snowflakeConn) Ping(ctx context.Context) error {
	logger.WithContext(ctx).Infoln("Ping")
	if sc.rest == nil {
		return driver.ErrBadConn
	}
	noResult := isAsyncMode(ctx)
	isDesc := isDescribeOnly(ctx)
	// TODO: handle isInternal
	_, err := sc.exec(ctx, "SELECT 1", noResult, false, /* isInternal */
		isDesc, []driver.NamedValue{})
	return err
}

// CheckNamedValue determines which types are handled by this driver aside from
// the instances captured by driver.Value
func (sc *snowflakeConn) CheckNamedValue(nv *driver.NamedValue) error {
	if _, ok := nv.Value.(SnowflakeDataType); ok {
		// Pass SnowflakeDataType args through without modification so that we can
		// distinguish them from arguments of type []byte
		return nil
	}
	if supported := supportedArrayBind(nv); !supported {
		return driver.ErrSkip
	}
	return nil
}

func (sc *snowflakeConn) GetQueryStatus(
	ctx context.Context,
	queryID string) (
	*SnowflakeQueryStatus, error) {
	queryRet, err := sc.checkQueryStatus(ctx, queryID)
	if err != nil {
		return nil, err
	}
	return &SnowflakeQueryStatus{
		queryRet.SQLText,
		queryRet.StartTime,
		queryRet.EndTime,
		queryRet.ErrorCode,
		queryRet.ErrorMessage,
		queryRet.Stats.ScanBytes,
		queryRet.Stats.ProducedRows,
		queryRet.Status,
	}, nil
}

// QueryArrowStream returns batches which can be queried for their raw arrow
// ipc stream of bytes. This way consumers don't need to be using the exact
// same version of Arrow as the connection is using internally in order
// to consume Arrow data.
func (sc *snowflakeConn) QueryArrowStream(ctx context.Context, query string, bindings ...driver.NamedValue) (ArrowStreamLoader, error) {
	ctx = WithArrowBatches(context.WithValue(ctx, asyncMode, false))
	ctx = setResultType(ctx, queryResultType)
	data, err := sc.exec(ctx, query, false, false /* isinternal */, false, bindings)
	if err != nil {
		logger.WithContext(ctx).Errorf("error: %v", err)
		if data != nil {
			code, err := strconv.Atoi(data.Code)
			if err != nil {
				return nil, err
			}
			return nil, (&SnowflakeError{
				Number:   code,
				SQLState: data.Data.SQLState,
				Message:  err.Error(),
				QueryID:  data.Data.QueryID,
			}).exceptionTelemetry(sc)
		}
		return nil, err
	}

	return &snowflakeArrowStreamChunkDownloader{
		sc:          sc,
		ChunkMetas:  data.Data.Chunks,
		Total:       data.Data.Total,
		Qrmk:        data.Data.Qrmk,
		ChunkHeader: data.Data.ChunkHeaders,
		FuncGet:     getChunk,
		RowSet: rowSetType{
			RowType:      data.Data.RowType,
			JSON:         data.Data.RowSet,
			RowSetBase64: data.Data.RowSetBase64,
		},
	}, nil
}

// ArrowStreamBatch is a type describing a potentially yet-to-be-downloaded
// Arrow IPC stream. Call `GetStream` to download and retrieve an io.Reader
// that can be used with ipc.NewReader to get record batch results.
type ArrowStreamBatch struct {
	idx     int
	numrows int64
	scd     *snowflakeArrowStreamChunkDownloader
	Loc     *time.Location
	rr      io.ReadCloser
}

// NumRows returns the total number of rows that the metadata stated should
// be in this stream of record batches.
func (asb *ArrowStreamBatch) NumRows() int64 { return asb.numrows }

// gzip.Reader.Close does NOT close the underlying reader, so we
// need to wrap with wrapReader so that closing will close the
// response body (or any other reader that we want to gzip uncompress)
type wrapReader struct {
	io.Reader
	wrapped io.ReadCloser
}

func (w *wrapReader) Close() error {
	if cl, ok := w.Reader.(io.ReadCloser); ok {
		if err := cl.Close(); err != nil {
			return err
		}
	}
	return w.wrapped.Close()
}

func (asb *ArrowStreamBatch) downloadChunkStreamHelper(ctx context.Context) error {
	headers := make(map[string]string)
	if len(asb.scd.ChunkHeader) > 0 {
		logger.Debug("chunk header is provided")
		for k, v := range asb.scd.ChunkHeader {
			logger.Debugf("adding header: %v, value: %v", k, v)

			headers[k] = v
		}
	} else {
		headers[headerSseCAlgorithm] = headerSseCAes
		headers[headerSseCKey] = asb.scd.Qrmk
	}

	resp, err := asb.scd.FuncGet(ctx, asb.scd.sc, asb.scd.ChunkMetas[asb.idx].URL, headers, asb.scd.sc.rest.RequestTimeout)
	if err != nil {
		return err
	}
	logger.Debugf("response returned chunk: %v for URL: %v", asb.idx+1, asb.scd.ChunkMetas[asb.idx].URL)
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		logger.Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, asb.scd.ChunkMetas[asb.idx].URL, b)
		logger.Infof("Header: %v", resp.Header)
		return &SnowflakeError{
			Number:      ErrFailedToGetChunk,
			SQLState:    SQLStateConnectionFailure,
			Message:     errMsgFailedToGetChunk,
			MessageArgs: []interface{}{asb.idx},
		}
	}

	defer func() {
		if asb.rr == nil {
			resp.Body.Close()
		}
	}()

	bufStream := bufio.NewReader(resp.Body)
	gzipMagic, err := bufStream.Peek(2)
	if err != nil {
		return err
	}

	if gzipMagic[0] == 0x1f && gzipMagic[1] == 0x8b {
		// detect and uncompress gzip
		bufStream0, err := gzip.NewReader(bufStream)
		if err != nil {
			return err
		}
		// gzip.Reader.Close() does NOT close the underlying
		// reader, so we need to wrap it and ensure close will
		// close the response body. Otherwise we'll leak it.
		asb.rr = &wrapReader{Reader: bufStream0, wrapped: resp.Body}
	} else {
		asb.rr = &wrapReader{Reader: bufStream, wrapped: resp.Body}
	}
	return nil
}

// GetStream returns a stream of bytes consisting of an Arrow IPC Record
// batch stream. Close should be called on the returned stream when done
// to ensure no leaked memory.
func (asb *ArrowStreamBatch) GetStream(ctx context.Context) (io.ReadCloser, error) {
	if asb.rr == nil {
		if err := asb.downloadChunkStreamHelper(ctx); err != nil {
			return nil, err
		}
	}

	return asb.rr, nil
}

// ArrowStreamLoader is a convenience interface for downloading
// Snowflake results via multiple Arrow Record Batch streams.
type ArrowStreamLoader interface {
	GetBatches() ([]ArrowStreamBatch, error)
	TotalRows() int64
	RowTypes() []execResponseRowType
	Location() *time.Location
}

type snowflakeArrowStreamChunkDownloader struct {
	sc          *snowflakeConn
	ChunkMetas  []execResponseChunk
	Total       int64
	Qrmk        string
	ChunkHeader map[string]string
	FuncGet     func(context.Context, *snowflakeConn, string, map[string]string, time.Duration) (*http.Response, error)
	RowSet      rowSetType
}

func (scd *snowflakeArrowStreamChunkDownloader) Location() *time.Location {
	if scd.sc != nil {
		return getCurrentLocation(scd.sc.cfg.Params)
	}
	return nil
}
func (scd *snowflakeArrowStreamChunkDownloader) TotalRows() int64 { return scd.Total }
func (scd *snowflakeArrowStreamChunkDownloader) RowTypes() []execResponseRowType {
	return scd.RowSet.RowType
}

// the server might have had an empty first batch, check if we can decode
// that first batch, if not we skip it.
func (scd *snowflakeArrowStreamChunkDownloader) maybeFirstBatch() []byte {
	if scd.RowSet.RowSetBase64 == "" {
		return nil
	}

	// first batch
	rowSetBytes, err := base64.StdEncoding.DecodeString(scd.RowSet.RowSetBase64)
	if err != nil {
		// match logic in buildFirstArrowChunk
		// assume there's no first chunk if we can't decode the base64 string
		return nil
	}

	// verify it's a valid ipc stream, otherwise skip it
	rr, err := ipc.NewReader(bytes.NewReader(rowSetBytes))
	if err != nil {
		return nil
	}
	rr.Release()

	return rowSetBytes
}

func (scd *snowflakeArrowStreamChunkDownloader) GetBatches() (out []ArrowStreamBatch, err error) {
	chunkMetaLen := len(scd.ChunkMetas)
	loc := scd.Location()

	out = make([]ArrowStreamBatch, chunkMetaLen, chunkMetaLen+1)
	toFill := out
	rowSetBytes := scd.maybeFirstBatch()
	// if there was no first batch in the response from the server,
	// skip it and move on. toFill == out
	// otherwise expand out by one to account for the first batch
	// and fill it in. have toFill refer to the slice of out excluding
	// the first batch.
	if len(rowSetBytes) > 0 {
		out = out[:chunkMetaLen+1]
		out[0] = ArrowStreamBatch{
			scd: scd,
			Loc: loc,
			rr:  io.NopCloser(bytes.NewReader(rowSetBytes)),
		}
		toFill = out[1:]
	}

	var totalCounted int64
	for i := range toFill {
		toFill[i] = ArrowStreamBatch{
			idx:     i,
			numrows: int64(scd.ChunkMetas[i].RowCount),
			Loc:     loc,
			scd:     scd,
		}
		totalCounted += int64(scd.ChunkMetas[i].RowCount)
	}

	if len(rowSetBytes) > 0 {
		// if we had a first batch, fill in the numrows
		out[0].numrows = scd.Total - totalCounted
	}
	return
}

func buildSnowflakeConn(ctx context.Context, config Config) (*snowflakeConn, error) {
	sc := &snowflakeConn{
		SequenceCounter: 0,
		ctx:             ctx,
		cfg:             &config,
	}
	var st http.RoundTripper = SnowflakeTransport
	if sc.cfg.Transporter == nil {
		if sc.cfg.InsecureMode {
			// no revocation check with OCSP. Think twice when you want to enable this option.
			st = snowflakeInsecureTransport
		} else {
			// set OCSP fail open mode
			ocspResponseCacheLock.Lock()
			atomic.StoreUint32((*uint32)(&ocspFailOpen), uint32(sc.cfg.OCSPFailOpen))
			ocspResponseCacheLock.Unlock()
		}
	} else {
		// use the custom transport
		st = sc.cfg.Transporter
	}
	if strings.HasSuffix(sc.cfg.Host, privateLinkSuffix) {
		if err := sc.setupOCSPPrivatelink(sc.cfg.Application, sc.cfg.Host); err != nil {
			return nil, err
		}
	} else {
		if _, set := os.LookupEnv(cacheServerURLEnv); set {
			os.Unsetenv(cacheServerURLEnv)
		}
	}
	var tokenAccessor TokenAccessor
	if sc.cfg.TokenAccessor != nil {
		tokenAccessor = sc.cfg.TokenAccessor
	} else {
		tokenAccessor = getSimpleTokenAccessor()
	}
	if sc.cfg.DisableTelemetry {
		sc.telemetry = &snowflakeTelemetry{enabled: false}
	}
	if sc.cfg.ConnectionID != "" {
		sc.execRespCache = acquireExecRespCache(sc.cfg.ConnectionID)
	}

	// authenticate
	sc.rest = &snowflakeRestful{
		Host:     sc.cfg.Host,
		Port:     sc.cfg.Port,
		Protocol: sc.cfg.Protocol,
		Client: &http.Client{
			// request timeout including reading response body
			Timeout:   sc.cfg.ClientTimeout,
			Transport: st,
		},
		TokenAccessor:       tokenAccessor,
		LoginTimeout:        sc.cfg.LoginTimeout,
		RequestTimeout:      sc.cfg.RequestTimeout,
		FuncPost:            postRestful,
		FuncGet:             getRestful,
		FuncPostQuery:       postRestfulQuery,
		FuncPostQueryHelper: postRestfulQueryHelper,
		FuncRenewSession:    renewRestfulSession,
		FuncPostAuth:        postAuth,
		FuncCloseSession:    closeSession,
		FuncCancelQuery:     cancelQuery,
		FuncPostAuthSAML:    postAuthSAML,
		FuncPostAuthOKTA:    postAuthOKTA,
		FuncGetSSO:          getSSO,
	}

	if sc.cfg.DisableTelemetry {
		sc.telemetry = &snowflakeTelemetry{enabled: false}
	} else {
		sc.telemetry = &snowflakeTelemetry{
			flushSize: defaultFlushSize,
			sr:        sc.rest,
			mutex:     &sync.Mutex{},
			enabled:   true,
		}
	}

	return sc, nil
}

// FetchResult returns a Rows handle for a previously issued query,
// given the snowflake query-id. This functionality is not used by the
// go sql library but is exported to clients who can make use of this
// capability explicitly.
//
// See the ResultFetcher interface.
func (sc *snowflakeConn) FetchResult(ctx context.Context, qid string) (driver.Rows, error) {
	return sc.buildRowsForRunningQuery(ctx, qid)
}

// WaitForQueryCompletion waits for the result of a previously issued query,
// given the snowflake query-id. This functionality is not used by the
// go sql library but is exported to clients who can make use of this
// capability explicitly.
func (sc *snowflakeConn) WaitForQueryCompletion(ctx context.Context, qid string) error {
	return sc.blockOnQueryCompletion(ctx, qid)
}

// ResultFetcher is an interface which allows a query result to be
// fetched given the corresponding snowflake query-id.
//
// The raw gosnowflake connection implements this interface and we
// export it so that clients can access this functionality, bypassing
// the alternative which is the query it via the RESULT_SCAN table
// function.
type ResultFetcher interface {
	FetchResult(ctx context.Context, qid string) (driver.Rows, error)
	WaitForQueryCompletion(ctx context.Context, qid string) error
}

// MonitoringResultFetcher is an interface which allows to fetch monitoringResult
// with snowflake connection and query-id.
type MonitoringResultFetcher interface {
	FetchMonitoringResult(queryID string, runtime time.Duration) (*monitoringResult, error)
}

// FetchMonitoringResult returns a monitoringResult object
// Multiplex can call monitoringResult.Monitoring() to get the QueryMonitoringData
func (sc *snowflakeConn) FetchMonitoringResult(queryID string, runtime time.Duration) (*monitoringResult, error) {
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}

	// set the fake runtime just to bypass fast query
	monitoringResult := mkMonitoringFetcher(sc, queryID, runtime)
	return monitoringResult, nil
}

// QuerySubmitter is an interface that allows executing a query synchronously
// while only fetching the result if the query completes within 45 seconds.
type QuerySubmitter interface {
	SubmitQuerySync(ctx context.Context, query string) (SnowflakeResult, error)
}

// SubmitQuerySync submits the given query for execution, and waits synchronously
// for up to 45 seconds.
// If the query complete within that duration, the SnowflakeResult is marked as complete,
// and the results can be fetched via the GetArrowBatches() method.
// Otherwise, the caller can use the provided query ID to fetch the query's results
// asynchronously. The caller must fetch the results of a query that is still running
// within 300 seconds, otherwise the query will be aborted.
func (sc *snowflakeConn) SubmitQuerySync(
	ctx context.Context,
	query string,
	args ...driver.NamedValue,
) (SnowflakeResult, error) {
	rows, err := sc.queryContextInternal(WithSubmitSync(WithArrowBatches(ctx)), query, args)
	if err != nil {
		return nil, err
	}

	return rows.(*snowflakeRows), nil
}

// TokenGetter is an interface that can be used to get the current tokens and session
// ID from a Snowflake connection. This returns the following values:
//   - token: The temporary credential used to authenticate requests to Snowflake's API.
//     This is valid for one hour.
//   - masterToken: Used to refresh the auth token above. Valid for four hours.
//   - sessionID: The ID of the Snowflake session corresponding to this connection.
type TokenGetter interface {
	GetTokens() (token string, masterToken string, sessionID int64)
}

func (sc *snowflakeConn) GetTokens() (token string, masterToken string, sessionID int64) {
	// TODO: If possible, check if the token will expire soon, and refresh it preemptively.
	return sc.rest.TokenAccessor.GetTokens()
}

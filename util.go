// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/arrow/memory"
)

type contextKey string

const (
	multiStatementCount      contextKey = "MULTI_STATEMENT_COUNT"
	asyncMode                contextKey = "ASYNC_MODE_QUERY"
	asyncModeNoFetch         contextKey = "ASYNC_MODE_NO_FETCH_QUERY"
	queryIDChannel           contextKey = "QUERY_ID_CHANNEL"
	snowflakeRequestIDKey    contextKey = "SNOWFLAKE_REQUEST_ID"
	fetchResultByID          contextKey = "SF_FETCH_RESULT_BY_ID"
	fileStreamFile           contextKey = "STREAMING_PUT_FILE"
	fileTransferOptions      contextKey = "FILE_TRANSFER_OPTIONS"
	enableHigherPrecision    contextKey = "ENABLE_HIGHER_PRECISION"
	arrowBatches             contextKey = "ARROW_BATCHES"
	arrowAlloc               contextKey = "ARROW_ALLOC"
	enableOriginalTimestamp  contextKey = "ENABLE_ORIGINAL_TIMESTAMP"
	queryTag                 contextKey = "QUERY_TAG"
	submitSync               contextKey = "SUBMIT_SYNC"
	reportAsyncError         contextKey = "REPORT_ASYNC_ERROR"
	skipCache                contextKey = "SKIP_CACHE"
	logSfResponseForCacheBug contextKey = "LOG_SF_RESPONSE_FOR_CACHE_BUG"
)

const (
	describeOnly        contextKey = "DESCRIBE_ONLY"
	cancelRetry         contextKey = "CANCEL_RETRY"
	streamChunkDownload contextKey = "STREAM_CHUNK_DOWNLOAD"
)

var (
	defaultTimeProvider = &unixTimeProvider{}
)

// WithMultiStatement returns a context that allows the user to execute the desired number of sql queries in one query
func WithMultiStatement(ctx context.Context, num int) (context.Context, error) {
	return context.WithValue(ctx, multiStatementCount, num), nil
}

// WithAsyncMode returns a context that allows execution of query in async mode
func WithAsyncMode(ctx context.Context) context.Context {
	return context.WithValue(ctx, asyncMode, true)
}

// WithAsyncModeNoFetch returns a context that, when you execute a query in async mode, will not fetch results
func WithAsyncModeNoFetch(ctx context.Context) context.Context {
	return context.WithValue(ctx, asyncModeNoFetch, true)
}

// WithQueryIDChan returns a context that contains the channel to receive the query ID
func WithQueryIDChan(ctx context.Context, c chan<- string) context.Context {
	return context.WithValue(ctx, queryIDChannel, c)
}

// WithRequestID returns a new context with the specified snowflake request id
func WithRequestID(ctx context.Context, requestID UUID) context.Context {
	return context.WithValue(ctx, snowflakeRequestIDKey, requestID)
}

// WithStreamDownloader returns a context that allows the use of a stream based chunk downloader
func WithStreamDownloader(ctx context.Context) context.Context {
	return context.WithValue(ctx, streamChunkDownload, true)
}

// WithFetchResultByID returns a context that allows retrieving the result by query ID
func WithFetchResultByID(ctx context.Context, queryID string) context.Context {
	return context.WithValue(ctx, fetchResultByID, queryID)
}

// WithFileStream returns a context that contains the address of the file stream to be PUT
func WithFileStream(ctx context.Context, reader io.Reader) context.Context {
	return context.WithValue(ctx, fileStreamFile, reader)
}

// WithFileTransferOptions returns a context that contains the address of file transfer options
func WithFileTransferOptions(ctx context.Context, options *SnowflakeFileTransferOptions) context.Context {
	return context.WithValue(ctx, fileTransferOptions, options)
}

// WithDescribeOnly returns a context that enables a describe only query
func WithDescribeOnly(ctx context.Context) context.Context {
	return context.WithValue(ctx, describeOnly, true)
}

// WithHigherPrecision returns a context that enables higher precision by
// returning a *big.Int or *big.Float variable when querying rows for column
// types with numbers that don't fit into its native Golang counterpart
func WithHigherPrecision(ctx context.Context) context.Context {
	return context.WithValue(ctx, enableHigherPrecision, true)
}

// WithArrowBatches returns a context that allows users to retrieve
// arrow.Record download workers upon querying
func WithArrowBatches(ctx context.Context) context.Context {
	return context.WithValue(ctx, arrowBatches, true)
}

// WithArrowAllocator returns a context embedding the provided allocator
// which will be utilized by chunk downloaders when constructing Arrow
// objects.
func WithArrowAllocator(ctx context.Context, pool memory.Allocator) context.Context {
	return context.WithValue(ctx, arrowAlloc, pool)
}

// WithOriginalTimestamp in combination with WithArrowBatches returns a context
// that allows users to retrieve arrow.Record with original timestamp struct returned by Snowflake.
// It can be used in case arrow.Timestamp cannot fit original timestamp values.
func WithOriginalTimestamp(ctx context.Context) context.Context {
	return context.WithValue(ctx, enableOriginalTimestamp, true)
}

// WithQueryTag returns a context that will set the given tag as the QUERY_TAG
// parameter on any queries that are run
func WithQueryTag(ctx context.Context, tag string) context.Context {
	return context.WithValue(ctx, queryTag, tag)
}

// WithSubmitSync returns a context that enables execution of a query that waits
// synchronously for the default timeout (up to 45 seconds), after which the client
// can poll for status using the query ID.
func WithSubmitSync(ctx context.Context) context.Context {
	return context.WithValue(ctx, submitSync, true)
}

// WithReportAsyncError returns a context that enables execution to panic and return
// any data that could be useful for debugging waitForCompletedQueryResultResp
func WithReportAsyncError(ctx context.Context) context.Context {
	return context.WithValue(ctx, reportAsyncError, true)
}

// WithSkipCache returns a context that enables execution to bypass the using the cache
// in multiplex, this can be set on a per org basis
// *** leave this in on rebase ***
func WithSkipCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipCache, true)
}

// WithLogSfResponseForCacheBug returns a context that enables execution to log sf result when success is not true but body is empty
// this is to help sf debug cache issue
// in multiplex, this can be set on a per org basis
// *** leave this in on rebase ***
func WithLogSfResponseForCacheBug(ctx context.Context) context.Context {
	return context.WithValue(ctx, logSfResponseForCacheBug, true)
}

// Get the request ID from the context if specified, otherwise generate one
func getOrGenerateRequestIDFromContext(ctx context.Context) UUID {
	requestID, ok := ctx.Value(snowflakeRequestIDKey).(UUID)
	if ok && requestID != nilUUID {
		return requestID
	}
	return NewUUID()
}

// integer min
func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// integer max
func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func int64Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func getMin(arr []int) int {
	if len(arr) == 0 {
		return -1
	}
	min := arr[0]
	for _, v := range arr {
		if v <= min {
			min = v
		}
	}
	return min
}

// time.Duration max
func durationMax(d1, d2 time.Duration) time.Duration {
	if d1-d2 > 0 {
		return d1
	}
	return d2
}

// time.Duration min
func durationMin(d1, d2 time.Duration) time.Duration {
	if d1-d2 < 0 {
		return d1
	}
	return d2
}

// toNamedValues converts a slice of driver.Value to a slice of driver.NamedValue for Go 1.8 SQL package
func toNamedValues(values []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(values))
	for idx, value := range values {
		namedValues[idx] = driver.NamedValue{Name: "", Ordinal: idx + 1, Value: value}
	}
	return namedValues
}

// TokenAccessor manages the session token and master token
type TokenAccessor interface {
	GetTokens() (token string, masterToken string, sessionID int64)
	SetTokens(token string, masterToken string, sessionID int64)
	Lock() error
	Unlock()
}

type simpleTokenAccessor struct {
	token        string
	masterToken  string
	sessionID    int64
	accessorLock sync.Mutex   // Used to implement accessor's Lock and Unlock
	tokenLock    sync.RWMutex // Used to synchronize SetTokens and GetTokens
}

// GetSimpleTokenAccessor returns an empty TokenAccessor.
func GetSimpleTokenAccessor() TokenAccessor {
	return getSimpleTokenAccessor()
}

func getSimpleTokenAccessor() TokenAccessor {
	return &simpleTokenAccessor{sessionID: -1}
}

func (sta *simpleTokenAccessor) Lock() error {
	sta.accessorLock.Lock()
	return nil
}

func (sta *simpleTokenAccessor) Unlock() {
	sta.accessorLock.Unlock()
}

func (sta *simpleTokenAccessor) GetTokens() (token string, masterToken string, sessionID int64) {
	sta.tokenLock.RLock()
	defer sta.tokenLock.RUnlock()
	return sta.token, sta.masterToken, sta.sessionID
}

func (sta *simpleTokenAccessor) SetTokens(token string, masterToken string, sessionID int64) {
	sta.tokenLock.Lock()
	defer sta.tokenLock.Unlock()
	sta.token = token
	sta.masterToken = masterToken
	sta.sessionID = sessionID
}

func escapeForCSV(value string) string {
	if value == "" {
		return "\"\""
	}
	if strings.Contains(value, "\"") || strings.Contains(value, "\n") ||
		strings.Contains(value, ",") || strings.Contains(value, "\\") {
		return "\"" + strings.ReplaceAll(value, "\"", "\"\"") + "\""
	}
	return value
}

// GetFromEnv is used to get the value of an environment variable from the system
func GetFromEnv(name string, failOnMissing bool) (string, error) {
	if value := os.Getenv(name); value != "" {
		return value, nil
	}
	if failOnMissing {
		return "", fmt.Errorf("%v environment variable is not set", name)
	}
	return "", nil
}

type currentTimeProvider interface {
	currentTime() int64
}

type unixTimeProvider struct {
}

func (utp *unixTimeProvider) currentTime() int64 {
	return time.Now().UnixMilli()
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}

func chooseRandomFromRange(min float64, max float64) float64 {
	return rand.Float64()*(max-min) + min
}

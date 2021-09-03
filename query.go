// Copyright (c) 2017-2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

//lint:file-ignore U1000 Ignore all unused code

import (
	"time"
)

type resultFormat string

const (
	jsonFormat  resultFormat = "json"
	arrowFormat resultFormat = "arrow"
)

type execBindParameter struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

type execRequest struct {
	SQLText      string                       `json:"sqlText"`
	AsyncExec    bool                         `json:"asyncExec"`
	SequenceID   uint64                       `json:"sequenceId"`
	IsInternal   bool                         `json:"isInternal"`
	DescribeOnly bool                         `json:"describeOnly,omitempty"`
	Parameters   map[string]interface{}       `json:"parameters,omitempty"`
	Bindings     map[string]execBindParameter `json:"bindings,omitempty"`
	BindStage    string                       `json:"bindStage,omitempty"`
}

type execResponseRowType struct {
	Name       string `json:"name"`
	ByteLength int64  `json:"byteLength"`
	Length     int64  `json:"length"`
	Type       string `json:"type"`
	Precision  int64  `json:"precision"`
	Scale      int64  `json:"scale"`
	Nullable   bool   `json:"nullable"`
}

type execResponseChunk struct {
	URL              string `json:"url"`
	RowCount         int    `json:"rowCount"`
	UncompressedSize int64  `json:"uncompressedSize"`
	CompressedSize   int64  `json:"compressedSize"`
}

type execResponseCredentials struct {
	AwsKeyID       string `json:"AWS_KEY_ID,omitempty"`
	AwsSecretKey   string `json:"AWS_SECRET_KEY,omitempty"`
	AwsToken       string `json:"AWS_TOKEN,omitempty"`
	AwsID          string `json:"AWS_ID,omitempty"`
	AwsKey         string `json:"AWS_KEY,omitempty"`
	AzureSasToken  string `json:"AZURE_SAS_TOKEN,omitempty"`
	GcsAccessToken string `json:"GCS_ACCESS_TOKEN,omitempty"`
}

type execResponseStageInfo struct {
	LocationType          string                  `json:"locationType,omitempty"`
	Location              string                  `json:"location,omitempty"`
	Path                  string                  `json:"path,omitempty"`
	Region                string                  `json:"region,omitempty"`
	StorageAccount        string                  `json:"storageAccount,omitempty"`
	IsClientSideEncrypted bool                    `json:"isClientSideEncrypted,omitempty"`
	Creds                 execResponseCredentials `json:"creds,omitempty"`
	PresignedURL          string                  `json:"presignedUrl,omitempty"`
	EndPoint              string                  `json:"endPoint,omitempty"`
}

// make all data field optional
type execResponseData struct {
	// succeed query response data
	Parameters         []nameValueParameter  `json:"parameters,omitempty"`
	RowType            []execResponseRowType `json:"rowtype,omitempty"`
	RowSet             [][]*string           `json:"rowset,omitempty"`
	RowSetBase64       string                `json:"rowsetbase64,omitempty"`
	Total              int64                 `json:"total,omitempty"`    // java:long
	Returned           int64                 `json:"returned,omitempty"` // java:long
	QueryID            string                `json:"queryId,omitempty"`
	SQLState           string                `json:"sqlState,omitempty"`
	DatabaseProvider   string                `json:"databaseProvider,omitempty"`
	FinalDatabaseName  string                `json:"finalDatabaseName,omitempty"`
	FinalSchemaName    string                `json:"finalSchemaName,omitempty"`
	FinalWarehouseName string                `json:"finalWarehouseName,omitempty"`
	FinalRoleName      string                `json:"finalRoleName,omitempty"`
	NumberOfBinds      int                   `json:"numberOfBinds,omitempty"`   // java:int
	StatementTypeID    int64                 `json:"statementTypeId,omitempty"` // java:long
	Version            int64                 `json:"version,omitempty"`         // java:long
	Chunks             []execResponseChunk   `json:"chunks,omitempty"`
	Qrmk               string                `json:"qrmk,omitempty"`
	ChunkHeaders       map[string]string     `json:"chunkHeaders,omitempty"`

	// ping pong response data
	GetResultURL      string        `json:"getResultUrl,omitempty"`
	ProgressDesc      string        `json:"progressDesc,omitempty"`
	QueryAbortTimeout time.Duration `json:"queryAbortsAfterSecs,omitempty"`
	ResultIDs         string        `json:"resultIds,omitempty"`
	ResultTypes       string        `json:"resultTypes,omitempty"`
	QueryResultFormat string        `json:"queryResultFormat,omitempty"`

	// async response placeholders
	AsyncResult *snowflakeResult `json:"asyncResult,omitempty"`
	AsyncRows   *snowflakeRows   `json:"asyncRows,omitempty"`

	// file transfer response data
	UploadInfo              execResponseStageInfo `json:"uploadInfo,omitempty"`
	LocalLocation           string                `json:"localLocation,omitempty"`
	SrcLocations            []string              `json:"src_locations,omitempty"`
	Parallel                int64                 `json:"parallel,omitempty"`
	Threshold               int64                 `json:"threshold,omitempty"`
	AutoCompress            bool                  `json:"autoCompress,omitempty"`
	Overwrite               bool                  `json:"overwrite,omitempty"`
	SourceCompression       string                `json:"sourceCompression,omitempty"`
	ShowEncryptionParameter bool                  `json:"clientShowEncryptionParameter,omitempty"`
	EncryptionMaterial      encryptionWrapper     `json:"encryptionMaterial,omitempty"`
	PresignedURLs           []string              `json:"presignedUrls,omitempty"`
	StageInfo               execResponseStageInfo `json:"stageInfo,omitempty"`
	Command                 string                `json:"command,omitempty"`
	Kind                    string                `json:"kind,omitempty"`
	Operation               string                `json:"operation,omitempty"`
}

type execResponse struct {
	Data    execResponseData `json:"Data"`
	Message string           `json:"message"`
	Code    string           `json:"code"`
	Success bool             `json:"success"`
}

// QueryMonitoringData is the struct returned by a request to /montitoring/queries/$qid
// Contains metadata about a query run
type QueryMonitoringData struct {
	ID                  string           `json:"id"`
	Status              string           `json:"status"`
	State               string           `json:"state"`
	ClientSendTime      int64            `json:"clientSendTime"`
	StartTime           int64            `json:"startTime"`
	EndTime             int64            `json:"endTime"`
	TotalDuration       int64            `json:"totalDuration"`
	ClusterNumber       int              `json:"clusterNumber"`
	WarehouseID         int              `json:"warehouseId"`
	WarehouseName       string           `json:"warehouseName"`
	WarehouseServerType string           `json:"warehouseServerType"`
	QueryTag            string           `json:"queryTag"`
	MajorVersionNumber  int              `json:"majorVersionNumber"`
	MinorVersionNumber  int              `json:"minorVersionNumber"`
	PatchVersionNumber  int              `json:"patchVersionNumber"`
	Stats               map[string]int64 `json:"stats"`
}

type monitoringResponse struct {
	Data struct {
		Queries []QueryMonitoringData `json:"queries"`
	} `json:"data"`
	Message string `json:"message"`
	Code    string `json:"code"`
	Success bool   `json:"success"`
}

type queryGraphResponse struct {
	Data    QueryGraphData `json:"data"`
	Message string         `json:"message"`
	Code    string         `json:"code"`
	Success bool           `json:"success"`
}

type QueryGraphData struct {
	Steps []QueryGraphStep `json:"steps"`
}

type QueryGraphStep struct {
	Step           int                `json:"step"`
	Description    string             `json:"description"`
	TimeInMs       int                `json:"timeInMs"`
	State          string             `json:"state"`
	ExecutionGraph ExecutionGraphData `json:"graphData"`
}

type ExecutionGraphData struct {
	Nodes  []ExecutionGraphNode  `json:"nodes"`
	Edges  []ExecutionGraphEdge  `json:"edges"`
	Global ExecutionGraphGlobals `json:"global"`
}

type ExecutionGraphNode struct {
	Id         int                      `json:"id"`
	LogicalId  string                   `json:"logicalId"`
	Name       string                   `json:"name"`
	Title      string                   `json:"title"`
	Statistics ExecutionGraphStatistics `json:"statistics"`
	Waits      []ExecutionGraphWait     `json:"waits"`
	TotalStats ExecutionGraphWait       `json:"totalStats"`
}

type ExecutionGraphEdge struct {
	Id   int `json:"id"`
	Src  int `json:"src"`
	Dst  int `json:"dst"`
	Rows int `json:"rows"`
}

type ExecutionGraphGlobals struct {
	Statistics ExecutionGraphStatistics `json:"statistics"`
	Waits      []ExecutionGraphWait     `json:"waits"`
	TotalStats ExecutionGraphWait       `json:"totalStats"`
}

type ExecutionGraphStatistics map[string][]ExecutionGraphStatistic

type ExecutionGraphStatistic struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
	Unit  string `json:"unit"`
}

type ExecutionGraphWait struct {
	Name       string  `json:"name"`
	Value      int     `json:"value"`
	Percentage float64 `json:"percentage"`
}

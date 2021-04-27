// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"database/sql/driver"
	"reflect"
	"strconv"
	"strings"
)

const (
	bindStageName   = "SYSTEM$BIND"
	createStageStmt = "CREATE TEMPORARY STAGE " + bindStageName + " file_format=" +
		"(type=csv field_optionally_enclosed_by='\"')"

	// size (in bytes) of max input stream (10MB default) as per JDBC specs
	inputStreamBufferSize = 1024 * 1024 * 10
)

type bindUploader struct {
	ctx            context.Context
	sc             *snowflakeConn
	stagePath      string
	fileCount      int
	arrayBindStage string
}

func (bu *bindUploader) upload(bindings []driver.NamedValue) (*execResponse, error) {
	bindingRows, _ := bu.buildRowsAsBytes(bindings)
	startIdx := 0
	numBytes := 0
	rowNum := 0
	bu.fileCount = 0
	var data *execResponse
	var err error
	for rowNum < len(bindingRows) {
		for numBytes < inputStreamBufferSize && rowNum < len(bindingRows) {
			numBytes += len(bindingRows[rowNum])
			rowNum++
		}
		// concatenate all byte arrays into 1 and put into input stream
		var b bytes.Buffer
		b.Grow(numBytes)
		for i := startIdx; i < rowNum; i++ {
			b.Write(bindingRows[i])
		}

		bu.fileCount++
		data, err = bu.uploadStreamInternal(&b, true)
		if err != nil {
			return nil, err
		}
		startIdx = rowNum
		numBytes = 0
	}
	return data, nil
}

func (bu *bindUploader) uploadStreamInternal(inputStream *bytes.Buffer, compressData bool) (*execResponse, error) {
	err := bu.createStageIfNeeded()
	if err != nil {
		return nil, err
	}
	stageName := bu.stagePath
	if stageName == "" {
		return nil, &SnowflakeError{
			Number:  ErrBindUpload,
			Message: "stage name is null",
		}
	}

	var putCommand strings.Builder
	// use a placeholder for source file
	putCommand.WriteString("put file:///tmp/placeholder ")
	// add stage name surrounded by quotations in case special chars are used in directory name
	putCommand.WriteString("'")
	putCommand.WriteString(stageName)
	putCommand.WriteString("'")
	putCommand.WriteString(" overwrite=true")
	// prepare context for PUT command
	ctx := WithFileStream(bu.ctx, inputStream)
	ctx = WithFileTransferOptions(ctx, &SnowflakeFileTransferOptions{
		compressSourceFromStream: compressData})
	return bu.sc.exec(
		ctx, putCommand.String(), false, true, false, []driver.NamedValue{})
}

func (bu *bindUploader) createStageIfNeeded() error {
	if bu.arrayBindStage != "" {
		return nil
	}
	data, err := bu.sc.exec(bu.ctx, createStageStmt, false, false, false, []driver.NamedValue{})
	if !data.Success {
		code, err := strconv.Atoi(data.Code)
		if err != nil {
			return err
		}
		return &SnowflakeError{
			Number:   code,
			SQLState: data.Data.SQLState,
			Message:  err.Error(),
			QueryID:  data.Data.QueryID}
	}
	if err != nil {
		return err
	}
	bu.arrayBindStage = bindStageName
	if err != nil {
		newThreshold := "0"
		bu.sc.cfg.Params[sessionArrayBindStageThreshold] = &newThreshold
	}
	return nil
}

// transpose the columns to rows and write them to a list of bytes
func (bu *bindUploader) buildRowsAsBytes(columns []driver.NamedValue) ([][]byte, error) {
	numColumns := len(columns)
	if columns[0].Value == nil {
		return nil, &SnowflakeError{
			Number:  ErrBindSerialization,
			Message: "no binds found in the first column",
		}
	}

	_, column := snowflakeArrayToString(&columns[0])
	numRows := len(column)
	csvRows := make([][]byte, 0)
	rows := make([][]string, 0)
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		rows = append(rows, make([]string, numColumns))
	}

	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		rows[rowIdx][0] = column[rowIdx]
	}
	for colIdx := 1; colIdx < numColumns; colIdx++ {
		_, column = snowflakeArrayToString(&columns[colIdx])
		iNumRows := len(column)
		if iNumRows != numRows {
			return nil, &SnowflakeError{
				Number:      ErrBindSerialization,
				Message:     errMsgBindColumnMismatch,
				MessageArgs: []interface{}{colIdx, iNumRows, numRows},
			}
		}
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			rows[rowIdx][colIdx] = column[rowIdx] // length of column = number of rows
		}
	}
	for _, row := range rows {
		csvRows = append(csvRows, bu.createCSVRecord(row))
	}
	return csvRows, nil
}

func (bu *bindUploader) createCSVRecord(data []string) []byte {
	var b strings.Builder
	b.Grow(1024)
	for i := 0; i < len(data); i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(escapeForCSV(data[i]))
	}
	b.WriteString("\n")
	return []byte(b.String())
}

func getBindValues(bindings []driver.NamedValue) (map[string]execBindParameter, error) {
	tsmode := timestampNtzType
	idx := 1
	var err error
	bindValues := make(map[string]execBindParameter, len(bindings))
	for _, binding := range bindings {
		t := goTypeToSnowflake(binding.Value, tsmode)
		if t == changeType {
			tsmode, err = dataTypeMode(binding.Value)
			if err != nil {
				return nil, err
			}
		} else {
			var val interface{}
			if t == sliceType {
				// retrieve array binding data
				t, val = snowflakeArrayToString(&binding)
			} else {
				val, err = valueToString(binding.Value, tsmode)
				if err != nil {
					return nil, err
				}
			}
			if t == nullType || t == unSupportedType {
				t = textType // if null or not supported, pass to GS as text
			}
			bindValues[strconv.Itoa(idx)] = execBindParameter{
				Type:  t.String(),
				Value: val,
			}
			idx++
		}
	}
	return bindValues, nil
}

func arrayBindValueCount(bindValues []driver.NamedValue) int {
	if !isArrayBind(bindValues) {
		return 0
	}
	_, arr := snowflakeArrayToString(&bindValues[0])
	return len(bindValues) * len(arr)
}

func isArrayBind(bindings []driver.NamedValue) bool {
	if len(bindings) == 0 {
		return false
	}
	for _, binding := range bindings {
		if supported := supportedArrayBind(&binding); !supported {
			return false
		}
	}
	return true
}

func supportedArrayBind(nv *driver.NamedValue) bool {
	switch reflect.TypeOf(nv.Value) {
	case reflect.TypeOf(&intArray{}), reflect.TypeOf(&int32Array{}),
		reflect.TypeOf(&int64Array{}), reflect.TypeOf(&float64Array{}),
		reflect.TypeOf(&float32Array{}), reflect.TypeOf(&boolArray{}),
		reflect.TypeOf(&stringArray{}), reflect.TypeOf(&byteArray{}):
		return true
	default:
		// TODO SNOW-292862 date, timestamp, time
		// TODO SNOW-176486 variant, object, array
		return false
	}
}

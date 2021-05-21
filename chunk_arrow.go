// Copyright (c) 2020-2020 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
"bytes"
"encoding/base64"
"fmt"
"github.com/apache/arrow/go/arrow/array"
"github.com/apache/arrow/go/arrow/ipc"
"github.com/apache/arrow/go/arrow/memory"
"io"
)

type arrowResultChunk struct {
	reader 				ipc.Reader
	rowCount			int64
	colCount			int64
	uncompressedSize	int
	allocator			memory.Allocator
}

func (arc *arrowResultChunk) decodeArrowChunk() ([]chunkRowType, error) {
	glog.V(2).Info("Arrow Decoder")

	var chunkRows []chunkRowType

	for {
		record, err := arc.reader.Read()
		if err == io.EOF {
			return chunkRows, nil
		} else if err != nil {
			return nil, err
		}

		numRows := int(record.NumRows())
		columns := record.Columns()
		chunkRows = make([]chunkRowType, numRows)

		for colIdx, col := range columns {
			destcol := make([]snowflakeValue, numRows)
			err := arrowToValue(&destcol, col)
			if err != nil {
				return nil, err
			}

			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				if colIdx == 0 {
					chunkRows[rowIdx] = chunkRowType{ArrowRow: make([]snowflakeValue, len(columns))}
				}
				chunkRows[rowIdx].ArrowRow[colIdx] = destcol[rowIdx]
			}
		}
		arc.rowCount += record.NumRows()
	}
	return chunkRows, nil
}

/**
Build arrow chunk based on RowSet of base64
*/
func buildFirstArrowChunk(rowsetBase64 string) arrowResultChunk {
	fmt.Println("build first arrow chunk")
	rowSetBytes, err := base64.StdEncoding.DecodeString(rowsetBase64)
	if err != nil {
		return arrowResultChunk{}
	}
	rr, err := ipc.NewReader(bytes.NewReader(rowSetBytes))
	if err != nil {
		return arrowResultChunk{}
	}

	return arrowResultChunk{*rr,0, 0, 0, memory.NewGoAllocator()}
}

func (arc *arrowResultChunk) mkError(s string) error {
	return fmt.Errorf("corrupt chunk: #{s}")
}

func (arc *arrowResultChunk) decode() ([]array.Record, error) {
	return make([]array.Record, defaultChunkBufferSize), nil

}
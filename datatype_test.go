// Copyright (c) 2017-2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"fmt"
	"testing"
)

type tcDataTypeMode struct {
	tp    SnowflakeDataType
	tmode snowflakeType
	err   error
}

func TestClientTypeToInternal(t *testing.T) {
	var testcases = []tcDataTypeMode{
		{tp: DataTypeFixed, tmode: fixedType, err: nil},
		{tp: DataTypeReal, tmode: realType, err: nil},
		{tp: DataTypeText, tmode: textType, err: nil},
		{tp: DataTypeDate, tmode: dateType, err: nil},
		{tp: DataTypeVariant, tmode: variantType, err: nil},
		{tp: DataTypeTimestampLtz, tmode: timestampLtzType, err: nil},
		{tp: DataTypeTimestampNtz, tmode: timestampNtzType, err: nil},
		{tp: DataTypeTimestampTz, tmode: timestampTzType, err: nil},
		{tp: DataTypeObject, tmode: objectType, err: nil},
		{tp: DataTypeArray, tmode: arrayType, err: nil},
		{tp: DataTypeBinary, tmode: binaryType, err: nil},
		{tp: DataTypeTime, tmode: timeType, err: nil},
		{tp: DataTypeBoolean, tmode: booleanType, err: nil},
		{tp: DataTypeNull, tmode: nullType, err: nil},
		{tp: nil, tmode: nullType,
			err: fmt.Errorf(errMsgInvalidByteArray, nil)},
	}
	for _, ts := range testcases {
		t.Run(fmt.Sprintf("%v_%v", ts.tp, ts.tmode), func(t *testing.T) {
			tmode, err := clientTypeToInternal(ts.tp)
			if ts.err == nil {
				if err != nil {
					t.Errorf("failed to get datatype mode: %v", err)
				}
				if tmode != ts.tmode {
					t.Errorf("wrong data type: %v", tmode)
				}
			} else {
				if err == nil {
					t.Errorf("should raise an error: %v", ts.err)
				}
			}
		})
	}
}

func TestPopulateSnowflakeParameter(t *testing.T) {
	columns := []string{"key", "value", "default", "level", "description", "set_by_user", "set_in_job", "set_on", "set_by_thread_id", "set_by_thread_name", "set_by_class", "parameter_comment", "type", "is_expired", "expires_at", "set_by_controlling_parameter", "activate_version", "partial_rollout"}
	p := SnowflakeParameter{}
	cols := make([]interface{}, len(columns))
	for i := 0; i < len(columns); i++ {
		cols[i] = populateSnowflakeParameter(columns[i], &p)
	}
	for i := 0; i < len(cols); i++ {
		if cols[i] == nil {
			t.Fatal("failed to populate parameter")
		}
	}
}

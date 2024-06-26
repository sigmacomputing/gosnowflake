// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"math/big"
	"math/cmplx"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/decimal128"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

func stringIntToDecimal(src string) (decimal128.Num, bool) {
	b, ok := new(big.Int).SetString(src, 10)
	if !ok {
		return decimal128.Num{}, ok
	}
	var high, low big.Int
	high.QuoRem(b, decimalShift, &low)
	return decimal128.New(high.Int64(), low.Uint64()), ok
}

func stringFloatToDecimal(src string, scale int64) (decimal128.Num, bool) {
	b, ok := new(big.Float).SetString(src)
	if !ok {
		return decimal128.Num{}, ok
	}
	s := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil))
	n := new(big.Float).Mul(b, s)
	if !n.IsInt() {
		return decimal128.Num{}, false
	}
	var high, low, z big.Int
	n.Int(&z)
	high.QuoRem(&z, decimalShift, &low)
	return decimal128.New(high.Int64(), low.Uint64()), ok
}

func stringFloatToInt(src string, scale int64) (int64, bool) {
	b, ok := new(big.Float).SetString(src)
	if !ok {
		return 0, ok
	}
	s := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil))
	n := new(big.Float).Mul(b, s)
	var z big.Int
	n.Int(&z)
	if !z.IsInt64() {
		return 0, false
	}
	return z.Int64(), true
}

type tcGoTypeToSnowflake struct {
	in    interface{}
	tmode SnowflakeDataType
	out   snowflakeType
}

func TestGoTypeToSnowflake(t *testing.T) {
	testcases := []tcGoTypeToSnowflake{
		{in: int64(123), tmode: nil, out: fixedType},
		{in: float64(234.56), tmode: nil, out: realType},
		{in: true, tmode: nil, out: booleanType},
		{in: "teststring", tmode: nil, out: textType},
		{in: Array([]int{1}), tmode: nil, out: sliceType},
		{in: time.Now(), tmode: nil, out: timestampNtzType},
		{in: time.Now(), tmode: DataTypeTimestampNtz, out: timestampNtzType},
		{in: time.Now(), tmode: DataTypeTimestampTz, out: timestampTzType},
		{in: time.Now(), tmode: DataTypeTimestampLtz, out: timestampLtzType},
		{in: []byte{1, 2, 3}, tmode: DataTypeBinary, out: binaryType},
		// Every explicit DataType should return changeType
		{in: DataTypeFixed, tmode: nil, out: changeType},
		{in: DataTypeReal, tmode: nil, out: changeType},
		{in: DataTypeText, tmode: nil, out: changeType},
		{in: DataTypeDate, tmode: nil, out: changeType},
		{in: DataTypeVariant, tmode: nil, out: changeType},
		{in: DataTypeTimestampLtz, tmode: nil, out: changeType},
		{in: DataTypeTimestampNtz, tmode: nil, out: changeType},
		{in: DataTypeTimestampTz, tmode: nil, out: changeType},
		{in: DataTypeObject, tmode: nil, out: changeType},
		{in: DataTypeArray, tmode: nil, out: changeType},
		{in: DataTypeBinary, tmode: nil, out: changeType},
		{in: DataTypeTime, tmode: nil, out: changeType},
		{in: DataTypeBoolean, tmode: nil, out: changeType},
		{in: DataTypeNull, tmode: nil, out: changeType},
		// negative
		{in: 123, tmode: nil, out: unSupportedType},
		{in: int8(12), tmode: nil, out: unSupportedType},
		{in: int32(456), tmode: nil, out: unSupportedType},
		{in: uint(456), tmode: nil, out: unSupportedType},
		{in: uint8(12), tmode: nil, out: unSupportedType},
		{in: uint64(456), tmode: nil, out: unSupportedType},
		{in: []byte{100}, tmode: nil, out: unSupportedType},
		{in: nil, tmode: nil, out: unSupportedType},
		{in: []int{1}, tmode: nil, out: unSupportedType},
	}
	for _, test := range testcases {
		t.Run(fmt.Sprintf("%v_%v_%v", test.in, test.out, test.tmode), func(t *testing.T) {
			a := goTypeToSnowflake(test.in, test.tmode)
			if a != test.out {
				t.Errorf("failed. in: %v, tmode: %v, expected: %v, got: %v", test.in, test.tmode, test.out, a)
			}
		})
	}
}

type tcSnowflakeTypeToGo struct {
	in    snowflakeType
	scale int64
	out   reflect.Type
}

func TestSnowflakeTypeToGo(t *testing.T) {
	testcases := []tcSnowflakeTypeToGo{
		{in: fixedType, scale: 0, out: reflect.TypeOf(int64(0))},
		{in: fixedType, scale: 2, out: reflect.TypeOf(float64(0))},
		{in: realType, scale: 0, out: reflect.TypeOf(float64(0))},
		{in: textType, scale: 0, out: reflect.TypeOf("")},
		{in: dateType, scale: 0, out: reflect.TypeOf(time.Now())},
		{in: timeType, scale: 0, out: reflect.TypeOf(time.Now())},
		{in: timestampLtzType, scale: 0, out: reflect.TypeOf(time.Now())},
		{in: timestampNtzType, scale: 0, out: reflect.TypeOf(time.Now())},
		{in: timestampTzType, scale: 0, out: reflect.TypeOf(time.Now())},
		{in: objectType, scale: 0, out: reflect.TypeOf("")},
		{in: variantType, scale: 0, out: reflect.TypeOf("")},
		{in: arrayType, scale: 0, out: reflect.TypeOf("")},
		{in: binaryType, scale: 0, out: reflect.TypeOf([]byte{})},
		{in: booleanType, scale: 0, out: reflect.TypeOf(true)},
		{in: sliceType, scale: 0, out: reflect.TypeOf("")},
	}
	for _, test := range testcases {
		t.Run(fmt.Sprintf("%v_%v", test.in, test.out), func(t *testing.T) {
			a := snowflakeTypeToGo(test.in, test.scale)
			if a != test.out {
				t.Errorf("failed. in: %v, scale: %v, expected: %v, got: %v",
					test.in, test.scale, test.out, a)
			}
		})
	}
}

func TestValueToString(t *testing.T) {
	v := cmplx.Sqrt(-5 + 12i) // should never happen as Go sql package must have already validated.
	_, err := valueToString(v, nil)
	if err == nil {
		t.Errorf("should raise error: %v", v)
	}

	// both localTime and utcTime should yield the same unix timestamp
	localTime := time.Date(2019, 2, 6, 14, 17, 31, 123456789, time.FixedZone("-08:00", -8*3600))
	utcTime := time.Date(2019, 2, 6, 22, 17, 31, 123456789, time.UTC)
	expectedUnixTime := "1549491451123456789" // time.Unix(1549491451, 123456789).Format(time.RFC3339) == "2019-02-06T14:17:31-08:00"
	expectedBool := "true"
	expectedInt64 := "1"
	expectedFloat64 := "1.1"
	expectedString := "teststring"

	if s, err := valueToString(localTime, DataTypeTimestampLtz); err != nil {
		t.Error("unexpected error")
	} else if s == nil {
		t.Errorf("expected '%v', got %v", expectedUnixTime, s)
	} else if *s != expectedUnixTime {
		t.Errorf("expected '%v', got '%v'", expectedUnixTime, *s)
	}

	if s, err := valueToString(utcTime, DataTypeTimestampLtz); err != nil {
		t.Error("unexpected error")
	} else if s == nil {
		t.Errorf("expected '%v', got %v", expectedUnixTime, s)
	} else if *s != expectedUnixTime {
		t.Errorf("expected '%v', got '%v'", expectedUnixTime, *s)
	}

	if s, err := valueToString(sql.NullBool{Bool: true, Valid: true}, DataTypeTimestampLtz); err != nil {
		t.Error("unexpected error")
	} else if s == nil {
		t.Errorf("expected '%v', got %v", expectedBool, s)
	} else if *s != expectedBool {
		t.Errorf("expected '%v', got '%v'", expectedBool, *s)
	}

	if s, err := valueToString(sql.NullInt64{Int64: 1, Valid: true}, DataTypeTimestampLtz); err != nil {
		t.Error("unexpected error")
	} else if s == nil {
		t.Errorf("expected '%v', got %v", expectedInt64, s)
	} else if *s != expectedInt64 {
		t.Errorf("expected '%v', got '%v'", expectedInt64, *s)
	}

	if s, err := valueToString(sql.NullFloat64{Float64: 1.1, Valid: true}, DataTypeTimestampLtz); err != nil {
		t.Error("unexpected error")
	} else if s == nil {
		t.Errorf("expected '%v', got %v", expectedFloat64, s)
	} else if *s != expectedFloat64 {
		t.Errorf("expected '%v', got '%v'", expectedFloat64, *s)
	}

	if s, err := valueToString(sql.NullString{String: "teststring", Valid: true}, DataTypeTimestampLtz); err != nil {
		t.Error("unexpected error")
	} else if s == nil {
		t.Errorf("expected '%v', got %v", expectedString, s)
	} else if *s != expectedString {
		t.Errorf("expected '%v', got '%v'", expectedString, *s)
	}
}

func TestExtractTimestamp(t *testing.T) {
	s := "1234abcdef" // pragma: allowlist secret
	_, _, err := extractTimestamp(&s)
	if err == nil {
		t.Errorf("should raise error: %v", s)
	}
	s = "1234abc.def"
	_, _, err = extractTimestamp(&s)
	if err == nil {
		t.Errorf("should raise error: %v", s)
	}
	s = "1234.def"
	_, _, err = extractTimestamp(&s)
	if err == nil {
		t.Errorf("should raise error: %v", s)
	}
}

func TestStringToValue(t *testing.T) {
	var source string
	var dest driver.Value
	var err error
	var rowType *execResponseRowType
	source = "abcdefg"

	types := []string{
		"date", "time", "timestamp_ntz", "timestamp_ltz", "timestamp_tz", "binary",
	}

	for _, tt := range types {
		t.Run(tt, func(t *testing.T) {
			rowType = &execResponseRowType{
				Type: tt,
			}
			if err = stringToValue(&dest, *rowType, &source, nil); err == nil {
				t.Errorf("should raise error. type: %v, value:%v", tt, source)
			}
		})
	}

	sources := []string{
		"12345K78 2020",
		"12345678 20T0",
	}

	types = []string{
		"timestamp_tz",
	}

	for _, ss := range sources {
		for _, tt := range types {
			t.Run(ss+tt, func(t *testing.T) {
				rowType = &execResponseRowType{
					Type: tt,
				}
				if err = stringToValue(&dest, *rowType, &ss, nil); err == nil {
					t.Errorf("should raise error. type: %v, value:%v", tt, source)
				}
			})
		}
	}

	src := "1549491451.123456789"
	if err = stringToValue(&dest, execResponseRowType{Type: "timestamp_ltz"}, &src, nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	} else if ts, ok := dest.(time.Time); !ok {
		t.Errorf("expected type: 'time.Time', got '%v'", reflect.TypeOf(dest))
	} else if ts.UnixNano() != 1549491451123456789 {
		t.Errorf("expected unix timestamp: 1549491451123456789, got %v", ts.UnixNano())
	}
}

type tcArrayToString struct {
	in  driver.NamedValue
	typ snowflakeType
	out []string
}

func TestArrayToString(t *testing.T) {
	testcases := []tcArrayToString{
		{in: driver.NamedValue{Value: &intArray{1, 2}}, typ: fixedType, out: []string{"1", "2"}},
		{in: driver.NamedValue{Value: &int32Array{1, 2}}, typ: fixedType, out: []string{"1", "2"}},
		{in: driver.NamedValue{Value: &int64Array{3, 4, 5}}, typ: fixedType, out: []string{"3", "4", "5"}},
		{in: driver.NamedValue{Value: &float64Array{6.7}}, typ: realType, out: []string{"6.7"}},
		{in: driver.NamedValue{Value: &float32Array{1.5}}, typ: realType, out: []string{"1.5"}},
		{in: driver.NamedValue{Value: &boolArray{true, false}}, typ: booleanType, out: []string{"true", "false"}},
		{in: driver.NamedValue{Value: &stringArray{"foo", "bar", "baz"}}, typ: textType, out: []string{"foo", "bar", "baz"}},
	}
	for _, test := range testcases {
		t.Run(strings.Join(test.out, "_"), func(t *testing.T) {
			s, a := snowflakeArrayToString(&test.in, false)
			if s != test.typ {
				t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.typ, s)
			}
			for i, v := range a {
				if *v != test.out[i] {
					t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.out[i], a)
				}
			}
		})
	}
}

func TestArrowToValue(t *testing.T) {
	dest := make([]snowflakeValue, 2)

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	var valids []bool // AppendValues() with an empty valid array adds every value by default

	localTime := time.Date(2019, 2, 6, 14, 17, 31, 123456789, time.FixedZone("-08:00", -8*3600))

	field1 := arrow.Field{Name: "epoch", Type: &arrow.Int64Type{}}
	field2 := arrow.Field{Name: "timezone", Type: &arrow.Int32Type{}}
	tzStruct := arrow.StructOf(field1, field2)

	type testObj struct {
		field1 int
		field2 string
	}

	for _, tc := range []struct {
		logical         string
		physical        string
		rowType         execResponseRowType
		values          interface{}
		builder         array.Builder
		append          func(b array.Builder, vs interface{})
		compare         func(src interface{}, dst []snowflakeValue) int
		higherPrecision bool
	}{
		{
			logical:         "fixed",
			physical:        "number", // default: number(38, 0)
			values:          []int64{1, 2},
			builder:         array.NewInt64Builder(pool),
			append:          func(b array.Builder, vs interface{}) { b.(*array.Int64Builder).AppendValues(vs.([]int64), valids) },
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "number(38,5)",
			rowType:  execResponseRowType{Scale: 5},
			values:   []string{"1.05430", "2.08983"},
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToInt(s, 5)
					if !ok {
						t.Fatalf("failed to convert to int")
					}
					b.(*array.Int64Builder).Append(num)
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToInt(srcvs[i], 5)
					if !ok {
						return i
					}
					srcDec := intToBigFloat(num, 5)
					dstDec := dst[i].(*big.Float)
					if srcDec.Cmp(dstDec) != 0 {
						return i
					}
				}
				return -1
			},
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "number(38,5)",
			rowType:  execResponseRowType{Scale: 5},
			values:   []string{"1.05430", "2.08983"},
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToInt(s, 5)
					if !ok {
						t.Fatalf("failed to convert to int")
					}
					b.(*array.Int64Builder).Append(num)
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToInt(srcvs[i], 5)
					if !ok {
						return i
					}
					srcDec := fmt.Sprintf("%.*f", 5, float64(num)/math.Pow10(int(5)))
					dstDec := dst[i]
					if srcDec != dstDec {
						return i
					}
				}
				return -1
			},
			higherPrecision: false,
		},
		{
			logical:  "fixed",
			physical: "number(38,0)",
			values:   []string{"10000000000000000000000000000000000000", "-12345678901234567890123456789012345678"},
			builder:  array.NewDecimal128Builder(pool, &arrow.Decimal128Type{Precision: 30, Scale: 2}),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringIntToDecimal(s)
					if !ok {
						t.Fatalf("failed to convert to big.Int")
					}
					b.(*array.Decimal128Builder).Append(num)
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringIntToDecimal(srcvs[i])
					if !ok {
						return i
					}
					srcDec := decimalToBigInt(num)
					dstDec := dst[i].(*big.Int)
					if srcDec.Cmp(dstDec) != 0 {
						return i
					}
				}
				return -1
			},
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "number(38,37)",
			rowType:  execResponseRowType{Scale: 37},
			values:   []string{"1.2345678901234567890123456789012345678", "-9.9999999999999999999999999999999999999"},
			builder:  array.NewDecimal128Builder(pool, &arrow.Decimal128Type{Precision: 38, Scale: 37}),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToDecimal(s, 37)
					if !ok {
						t.Fatalf("failed to convert to big.Rat")
					}
					b.(*array.Decimal128Builder).Append(num)
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToDecimal(srcvs[i], 37)
					if !ok {
						return i
					}
					srcDec := decimalToBigFloat(num, 37)
					dstDec := dst[i].(*big.Float)
					if srcDec.Cmp(dstDec) != 0 {
						return i
					}
				}
				return -1
			},
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "int8",
			values:   []int8{1, 2},
			builder:  array.NewInt8Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int8Builder).AppendValues(vs.([]int8), valids) },
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]int8)
				for i := range srcvs {
					if int64(srcvs[i]) != dst[i].(int64) {
						return i
					}
				}
				return -1
			},
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "int16",
			values:   []int16{1, 2},
			builder:  array.NewInt16Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int16Builder).AppendValues(vs.([]int16), valids) },
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]int16)
				for i := range srcvs {
					if int64(srcvs[i]) != dst[i].(int64) {
						return i
					}
				}
				return -1
			},
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "int16",
			values:   []string{"1.2345", "2.3456"},
			rowType:  execResponseRowType{Scale: 4},
			builder:  array.NewInt16Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToInt(s, 4)
					if !ok {
						t.Fatalf("failed to convert to int")
					}
					b.(*array.Int16Builder).Append(int16(num))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToInt(srcvs[i], 4)
					if !ok {
						return i
					}
					srcDec := intToBigFloat(num, 4)
					dstDec := dst[i].(*big.Float)
					if srcDec.Cmp(dstDec) != 0 {
						return i
					}
				}
				return -1
			},
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "int16",
			values:   []string{"1.2345", "2.3456"},
			rowType:  execResponseRowType{Scale: 4},
			builder:  array.NewInt16Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToInt(s, 4)
					if !ok {
						t.Fatalf("failed to convert to int")
					}
					b.(*array.Int16Builder).Append(int16(num))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToInt(srcvs[i], 4)
					if !ok {
						return i
					}
					srcDec := fmt.Sprintf("%.*f", 4, float64(num)/math.Pow10(int(4)))
					dstDec := dst[i]
					if srcDec != dstDec {
						return i
					}
				}
				return -1
			},
			higherPrecision: false,
		},
		{
			logical:  "fixed",
			physical: "int32",
			values:   []int32{1, 2},
			builder:  array.NewInt32Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int32Builder).AppendValues(vs.([]int32), valids) },
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]int32)
				for i := range srcvs {
					if int64(srcvs[i]) != dst[i].(int64) {
						return i
					}
				}
				return -1
			},
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "int32",
			values:   []string{"1.23456", "2.34567"},
			rowType:  execResponseRowType{Scale: 5},
			builder:  array.NewInt32Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToInt(s, 5)
					if !ok {
						t.Fatalf("failed to convert to int")
					}
					b.(*array.Int32Builder).Append(int32(num))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToInt(srcvs[i], 5)
					if !ok {
						return i
					}
					srcDec := intToBigFloat(num, 5)
					dstDec := dst[i].(*big.Float)
					if srcDec.Cmp(dstDec) != 0 {
						return i
					}
				}
				return -1
			},
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "int32",
			values:   []string{"1.23456", "2.34567"},
			rowType:  execResponseRowType{Scale: 5},
			builder:  array.NewInt32Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToInt(s, 5)
					if !ok {
						t.Fatalf("failed to convert to int")
					}
					b.(*array.Int32Builder).Append(int32(num))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToInt(srcvs[i], 5)
					if !ok {
						return i
					}
					srcDec := fmt.Sprintf("%.*f", 5, float64(num)/math.Pow10(int(5)))
					dstDec := dst[i]
					if srcDec != dstDec {
						return i
					}
				}
				return -1
			},
			higherPrecision: false,
		},
		{
			logical:         "fixed",
			physical:        "int64",
			values:          []int64{1, 2},
			builder:         array.NewInt64Builder(pool),
			append:          func(b array.Builder, vs interface{}) { b.(*array.Int64Builder).AppendValues(vs.([]int64), valids) },
			higherPrecision: true,
		},
		{
			logical: "boolean",
			values:  []bool{true, false},
			builder: array.NewBooleanBuilder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.BooleanBuilder).AppendValues(vs.([]bool), valids) },
		},
		{
			logical:  "real",
			physical: "float",
			values:   []float64{1, 2},
			builder:  array.NewFloat64Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Float64Builder).AppendValues(vs.([]float64), valids) },
		},
		{
			logical:  "text",
			physical: "string",
			values:   []string{"foo", "bar"},
			builder:  array.NewStringBuilder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.StringBuilder).AppendValues(vs.([]string), valids) },
		},
		{
			logical: "binary",
			values:  [][]byte{[]byte("foo"), []byte("bar")},
			builder: array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary),
			append:  func(b array.Builder, vs interface{}) { b.(*array.BinaryBuilder).AppendValues(vs.([][]byte), valids) },
		},
		{
			logical: "date",
			values:  []time.Time{time.Now(), localTime},
			builder: array.NewDate32Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, d := range vs.([]time.Time) {
					b.(*array.Date32Builder).Append(arrow.Date32(d.Unix()))
				}
			},
		},
		{
			logical: "time",
			values:  []time.Time{time.Now(), time.Now()},
			rowType: execResponseRowType{Scale: 9},
			builder: array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano())
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i := range srcvs {
					if srcvs[i].Nanosecond() != dst[i].(time.Time).Nanosecond() {
						return i
					}
				}
				return -1
			},
			higherPrecision: true,
		},
		{
			logical: "timestamp_ntz",
			values:  []time.Time{time.Now(), localTime},
			rowType: execResponseRowType{Scale: 9},
			builder: array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano())
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i := range srcvs {
					if srcvs[i].UnixNano() != dst[i].(time.Time).UnixNano() {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_ntz",
			values:  []time.Time{time.Now(), localTime},
			rowType: execResponseRowType{Scale: 3},
			builder: array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano() / 1000000)
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i := range srcvs {
					if srcvs[i].UnixNano()/1000000 != dst[i].(time.Time).UnixNano()/1000000 {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_ltz",
			values:  []time.Time{time.Now(), localTime},
			rowType: execResponseRowType{Scale: 9},
			builder: array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano())
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i := range srcvs {
					if srcvs[i].UnixNano() != dst[i].(time.Time).UnixNano() {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_ltz",
			values:  []time.Time{time.Now(), localTime},
			rowType: execResponseRowType{Scale: 3},
			builder: array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano() / 1000000)
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i := range srcvs {
					if srcvs[i].UnixNano()/1000000 != dst[i].(time.Time).UnixNano()/1000000 {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_tz",
			values:  []time.Time{time.Now(), localTime},
			builder: array.NewStructBuilder(pool, tzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.UnixNano()))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i := range srcvs {
					if srcvs[i].Unix() != dst[i].(time.Time).Unix() {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "array",
			values:  [][]string{{"foo", "bar"}, {"baz", "quz", "quux"}},
			builder: array.NewStringBuilder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, a := range vs.([][]string) {
					b.(*array.StringBuilder).Append(fmt.Sprint(a))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([][]string)
				for i, o := range srcvs {
					if fmt.Sprint(o) != dst[i].(string) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "object",
			values:  []testObj{{0, "foo"}, {1, "bar"}},
			builder: array.NewStringBuilder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, o := range vs.([]testObj) {
					b.(*array.StringBuilder).Append(fmt.Sprint(o))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]testObj)
				for i, o := range srcvs {
					if fmt.Sprint(o) != dst[i].(string) {
						return i
					}
				}
				return -1
			},
		},
	} {
		testName := tc.logical
		if tc.physical != "" {
			testName += " " + tc.physical
		}
		t.Run(testName, func(t *testing.T) {
			b := tc.builder
			tc.append(b, tc.values)
			arr := b.NewArray()
			defer arr.Release()

			meta := tc.rowType
			meta.Type = tc.logical

			withHigherPrecision := tc.higherPrecision

			if err := arrowToValue(dest, meta, arr, localTime.Location(), withHigherPrecision); err != nil {
				t.Fatalf("error: %s", err)
			}

			elemType := reflect.TypeOf(tc.values).Elem()
			if tc.compare != nil {
				idx := tc.compare(tc.values, dest)
				if idx != -1 {
					t.Fatalf("error: column array value mistmatch at index %v", idx)
				}
			} else {
				for _, d := range dest {
					if reflect.TypeOf(d) != elemType {
						t.Fatalf("error: expected type %s, got type %s", reflect.TypeOf(d), elemType)
					}
				}
			}
		})

	}
}

func TestArrowToRecord(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0) // ensure no arrow memory leaks
	var valids []bool           // AppendValues() with an empty valid array adds every value by default

	localTime := time.Date(2019, 2, 6, 14, 17, 31, 123456789, time.FixedZone("-08:00", -8*3600))

	field1 := arrow.Field{Name: "epoch", Type: &arrow.Int64Type{}}
	field2 := arrow.Field{Name: "timezone", Type: &arrow.Int32Type{}}
	tzStruct := arrow.StructOf(field1, field2)

	type testObj struct {
		field1 int
		field2 string
	}

	for _, tc := range []struct {
		logical  string
		physical string
		sc       *arrow.Schema
		rowType  execResponseRowType
		values   interface{}
		error    string
		origTS   bool
		nrows    int
		builder  array.Builder
		append   func(b array.Builder, vs interface{})
		compare  func(src interface{}, rec arrow.Record) int
	}{
		{
			logical: "boolean",
			sc:      arrow.NewSchema([]arrow.Field{{Type: &arrow.BooleanType{}}}, nil),
			values:  []bool{true, false},
			nrows:   2,
			builder: array.NewBooleanBuilder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.BooleanBuilder).AppendValues(vs.([]bool), valids) },
		},
		{
			logical:  "real",
			physical: "float",
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Float64Type{}}}, nil),
			values:   []float64{1, 2},
			nrows:    2,
			builder:  array.NewFloat64Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Float64Builder).AppendValues(vs.([]float64), valids) },
		},
		{
			logical:  "text",
			physical: "string",
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.StringType{}}}, nil),
			values:   []string{"foo", "bar"},
			nrows:    2,
			builder:  array.NewStringBuilder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.StringBuilder).AppendValues(vs.([]string), valids) },
		},
		{
			logical: "binary",
			sc:      arrow.NewSchema([]arrow.Field{{Type: &arrow.BinaryType{}}}, nil),
			values:  [][]byte{[]byte("foo"), []byte("bar")},
			nrows:   2,
			builder: array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary),
			append:  func(b array.Builder, vs interface{}) { b.(*array.BinaryBuilder).AppendValues(vs.([][]byte), valids) },
		},
		{
			logical: "date",
			sc:      arrow.NewSchema([]arrow.Field{{Type: &arrow.Date32Type{}}}, nil),
			values:  []time.Time{time.Now(), localTime},
			nrows:   2,
			builder: array.NewDate32Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, d := range vs.([]time.Time) {
					b.(*array.Date32Builder).Append(arrow.Date32(d.Unix()))
				}
			},
		},
		{
			logical: "time",
			sc:      arrow.NewSchema([]arrow.Field{{Type: arrow.FixedWidthTypes.Time64ns}}, nil),
			values:  []time.Time{time.Now(), time.Now()},
			nrows:   2,
			builder: array.NewTime64Builder(pool, arrow.FixedWidthTypes.Time64ns.(*arrow.Time64Type)),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Time64Builder).Append(arrow.Time64(t.UnixNano()))
				}
			},
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				arr := convertedRec.Column(0).(*array.Time64)
				for i := 0; i < arr.Len(); i++ {
					if srcvs[i].UnixNano() != int64(arr.Value(i)) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "timestamp_ntz",
			physical: "int64",                                                                                  // timestamp_ntz with scale 0..3 -> int64
			values:   []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond)}, // Millisecond for scale = 3
			nrows:    2,
			rowType:  execResponseRowType{Scale: 3},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixMilli()) // Millisecond for scale = 3
				}
			},
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if srcvs[i].UnixMicro() != int64(t) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "timestamp_ntz",
			physical: "int64",
			values:   []time.Time{time.Now(), localTime},
			nrows:    2,
			rowType:  execResponseRowType{Scale: 9},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano())
				}
			},
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if srcvs[i].UnixMicro() != int64(t) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_ltz",
			values:  []time.Time{time.Now(), localTime},
			nrows:   2,
			rowType: execResponseRowType{Scale: 9},
			sc:      arrow.NewSchema([]arrow.Field{{Type: &arrow.TimestampType{}}}, nil),
			builder: array.NewTimestampBuilder(pool, &arrow.TimestampType{}),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.TimestampBuilder).Append(arrow.Timestamp(t.UnixNano()))
				}
			},
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if srcvs[i].UnixMicro() != int64(t) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "timestamp_ltz",
			physical: "int64",
			values:   []time.Time{time.Now(), localTime},
			nrows:    2,
			rowType:  execResponseRowType{Scale: 9},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano())
				}
			},
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if srcvs[i].UnixMicro() != int64(t) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_tz",
			values:  []time.Time{time.Now(), localTime},
			nrows:   2,
			sc:      arrow.NewSchema([]arrow.Field{{Type: arrow.StructOf(field1, field2)}}, nil),
			builder: array.NewStructBuilder(pool, tzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
				}
			},
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if srcvs[i].Unix() != time.Unix(0, int64(t)*1000).Unix() {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "array",
			values:  [][]string{{"foo", "bar"}, {"baz", "quz", "quux"}},
			nrows:   2,
			sc:      arrow.NewSchema([]arrow.Field{{Type: &arrow.StringType{}}}, nil),
			builder: array.NewStringBuilder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, a := range vs.([][]string) {
					b.(*array.StringBuilder).Append(fmt.Sprint(a))
				}
			},
		},
		{
			logical: "object",
			values:  []testObj{{0, "foo"}, {1, "bar"}},
			nrows:   2,
			sc:      arrow.NewSchema([]arrow.Field{{Type: &arrow.StringType{}}}, nil),
			builder: array.NewStringBuilder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, o := range vs.([]testObj) {
					b.(*array.StringBuilder).Append(fmt.Sprint(o))
				}
			},
		},
	} {
		testName := tc.logical
		if tc.physical != "" {
			testName += " " + tc.physical
		}
		t.Run(testName, func(t *testing.T) {
			scope := memory.NewCheckedAllocatorScope(pool)
			defer scope.CheckSize(t)

			b := tc.builder
			defer b.Release()
			tc.append(b, tc.values)
			arr := b.NewArray()
			defer arr.Release()
			rawRec := array.NewRecord(tc.sc, []arrow.Array{arr}, int64(tc.nrows))
			defer rawRec.Release()

			meta := tc.rowType
			meta.Type = tc.logical

			ctx := context.Background()
			if tc.origTS {
				ctx = WithOriginalTimestamp(ctx)
			}

			transformedRec, err := arrowToRecord(ctx, rawRec, pool, []execResponseRowType{meta}, localTime.Location())
			if err != nil {
				if tc.error == "" || !strings.Contains(err.Error(), tc.error) {
					t.Fatalf("error: %s", err)
				}
			} else {
				defer transformedRec.Release()
				if tc.error != "" {
					t.Fatalf("expected error: %s", tc.error)
				}

				if tc.compare != nil {
					idx := tc.compare(tc.values, transformedRec)
					if idx != -1 {
						t.Fatalf("error: column array value mismatch at index %v", idx)
					}
				} else {
					for i, c := range transformedRec.Columns() {
						rawCol := rawRec.Column(i)
						if rawCol != c {
							t.Fatalf("error: expected column %s, got column %s", rawCol, c)
						}
					}
				}
			}
		})
	}
}

func TestTimestampLTZLocation(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		src := "1549491451.123456789"
		var dest driver.Value
		loc, _ := time.LoadLocation(PSTLocation)
		if err := stringToValue(&dest, execResponseRowType{Type: "timestamp_ltz"}, &src, loc); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		ts, ok := dest.(time.Time)
		if !ok {
			t.Errorf("expected type: 'time.Time', got '%v'", reflect.TypeOf(dest))
		}
		if ts.Location() != loc {
			t.Errorf("expected location to be %v, got '%v'", loc, ts.Location())
		}

		if err := stringToValue(&dest, execResponseRowType{Type: "timestamp_ltz"}, &src, nil); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		ts, ok = dest.(time.Time)
		if !ok {
			t.Errorf("expected type: 'time.Time', got '%v'", reflect.TypeOf(dest))
		}
		if ts.Location() != time.Local {
			t.Errorf("expected location to be local, got '%v'", ts.Location())
		}
	})
}

func TestSmallTimestampBinding(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		ctx := context.Background()
		timeValue, err := time.Parse("2006-01-02 15:04:05", "1600-10-10 10:10:10")
		if err != nil {
			t.Fatalf("failed to parse time: %v", err)
		}
		parameters := []driver.NamedValue{
			{Ordinal: 1, Value: DataTypeTimestampNtz},
			{Ordinal: 2, Value: timeValue},
		}

		rows := sct.mustQueryContext(ctx, "SELECT ?", parameters)
		defer rows.Close()

		scanValues := make([]driver.Value, 1)
		for {
			if err := rows.Next(scanValues); err == io.EOF {
				break
			} else if err != nil {
				t.Fatalf("failed to run query: %v", err)
			}
			if scanValues[0] != timeValue {
				t.Fatalf("unexpected result. expected: %v, got: %v", timeValue, scanValues[0])
			}
		}
	})
}

func TestTimestampConversionWithoutArrowBatches(t *testing.T) {
	timestamps := [3]string{
		"2000-10-10 10:10:10.123456789", // neutral
		"9999-12-12 23:59:59.999999999", // max
		"0001-01-01 00:00:00.000000000"} // min
	types := [3]string{"TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}

	runDBTest(t, func(sct *DBTest) {
		// (sigma rebase): skip these tests for now since they are new and not yet supported by our custom conversion logic
		// TODO (sigma rebase): re-enable these tests
		t.Skip()
		ctx := context.Background()

		for _, tsStr := range timestamps {
			ts, err := time.Parse("2006-01-02 15:04:05", tsStr)
			if err != nil {
				t.Fatalf("failed to parse time: %v", err)
			}
			for _, tp := range types {
				for scale := 0; scale <= 9; scale++ {
					t.Run(tp+"("+strconv.Itoa(scale)+")_"+tsStr, func(t *testing.T) {
						query := fmt.Sprintf("SELECT '%s'::%s(%v)", tsStr, tp, scale)
						rows := sct.mustQueryContext(ctx, query, nil)
						defer rows.Close()

						if rows.Next() {
							var act time.Time
							rows.Scan(&act)
							exp := ts.Truncate(time.Duration(math.Pow10(9 - scale)))
							if !exp.Equal(act) {
								t.Fatalf("unexpected result. expected: %v, got: %v", exp, act)
							}
						} else {
							t.Fatalf("failed to run query: %v", query)
						}
					})
				}
			}
		}
	})
}

func TestTimestampConversionWithArrowBatchesFailsForDistantDates(t *testing.T) {
	timestamps := [2]string{
		"9999-12-12 23:59:59.999999999", // max
		"0001-01-01 00:00:00.000000000"} // min
	types := [3]string{"TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}

	expectedError := "Cannot convert timestamp"

	runSnowflakeConnTest(t, func(sct *SCTest) {
		// (sigma rebase): skip these tests for now since they are new and not yet supported by our custom conversion logic
		// TODO (sigma rebase): re-enable these tests
		t.Skip()
		ctx := WithArrowBatches(sct.sc.ctx)

		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx = WithArrowAllocator(ctx, pool)

		for _, tsStr := range timestamps {
			for _, tp := range types {
				for scale := 0; scale <= 9; scale++ {
					t.Run(tp+"("+strconv.Itoa(scale)+")_"+tsStr, func(t *testing.T) {

						query := fmt.Sprintf("SELECT '%s'::%s(%v)", tsStr, tp, scale)
						_, err := sct.sc.QueryContext(ctx, query, []driver.NamedValue{})
						if err != nil {
							if !strings.Contains(err.Error(), expectedError) {
								t.Fatalf("improper error, expected: %v, got: %v", expectedError, err.Error())
							}
						} else {
							t.Fatalf("no error, expected: %v ", expectedError)

						}
					})
				}
			}
		}
	})
}

func TestTimestampConversionWithArrowBatchesAndWithOriginalTimestamp(t *testing.T) {
	timestamps := [3]string{
		"2000-10-10 10:10:10.123456789", // neutral
		"9999-12-12 23:59:59.999999999", // max
		"0001-01-01 00:00:00.000000000"} // min
	types := [3]string{"TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}

	runSnowflakeConnTest(t, func(sct *SCTest) {
		ctx := WithOriginalTimestamp(WithArrowBatches(sct.sc.ctx))
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx = WithArrowAllocator(ctx, pool)

		for _, tsStr := range timestamps {
			ts, err := time.Parse("2006-01-02 15:04:05", tsStr)
			if err != nil {
				t.Fatalf("failed to parse time: %v", err)
			}
			for _, tp := range types {
				for scale := 0; scale <= 9; scale++ {
					t.Run(tp+"("+strconv.Itoa(scale)+")_"+tsStr, func(t *testing.T) {

						query := fmt.Sprintf("SELECT '%s'::%s(%v)", tsStr, tp, scale)
						rows := sct.mustQueryContext(ctx, query, []driver.NamedValue{})
						defer rows.Close()

						// getting result batches
						batches, err := rows.(*snowflakeRows).GetArrowBatches()
						if err != nil {
							t.Error(err)
						}

						numBatches := len(batches)
						if numBatches != 1 {
							t.Errorf("incorrect number of batches, expected: 1, got: %v", numBatches)
						}

						rec, err := batches[0].Fetch()
						if err != nil {
							t.Error(err)
						}
						exp := ts.Truncate(time.Duration(math.Pow10(9 - scale)))
						for _, r := range *rec {
							defer r.Release()
							act := batches[0].ArrowSnowflakeTimestampToTime(r, 0, 0)
							if act == nil {
								t.Fatalf("unexpected result. expected: %v, got: nil", exp)
							} else if !exp.Equal(*act) {
								t.Fatalf("unexpected result. expected: %v, got: %v", exp, act)
							}
						}
					})
				}
			}
		}
	})
}

func TestTimeTypeValueToString(t *testing.T) {
	timeValue, err := time.Parse("2006-01-02 15:04:05", "2020-01-02 10:11:12")
	if err != nil {
		t.Fatal(err)
	}
	offsetTimeValue, err := time.ParseInLocation("2006-01-02 15:04:05", "2020-01-02 10:11:12", Location(6*60))
	if err != nil {
		t.Fatal(err)
	}

	testcases := []struct {
		in       time.Time
		dataType SnowflakeDataType
		out      string
	}{
		{timeValue, DataTypeDate, "1577959872000"},
		{timeValue, DataTypeTime, "36672000000000"},
		{timeValue, DataTypeTimestampNtz, "1577959872000000000"},
		{timeValue, DataTypeTimestampLtz, "1577959872000000000"},
		{timeValue, DataTypeTimestampTz, "1577959872000000000 1440"},
		{offsetTimeValue, DataTypeTimestampTz, "1577938272000000000 1800"},
	}

	for _, tc := range testcases {
		t.Run(tc.out, func(t *testing.T) {
			output, err := timeTypeValueToString(tc.in, tc.dataType)
			if err != nil {
				t.Error(err)
			}
			if strings.Compare(tc.out, *output) != 0 {
				t.Errorf("failed to convert time %v of type %v. expected: %v, received: %v", tc.in, tc.dataType, tc.out, *output)
			}
		})
	}
}

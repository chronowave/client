package decode

import (
	"encoding/json"
	"fmt"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"

	"github.com/chronowave/client/go/internal/errors"
	"github.com/chronowave/client/go/internal/runtime"
)

type unmarshalJSONDecoder struct {
	typ        *runtime.Type
	structName string
	fieldName  string
}

func newUnmarshalJSONDecoder(typ *runtime.Type, structName, fieldName string) *unmarshalJSONDecoder {
	return &unmarshalJSONDecoder{
		typ:        typ,
		structName: structName,
		fieldName:  fieldName,
	}
}

func (d *unmarshalJSONDecoder) annotateError(cursor int64, err error) {
	switch e := err.(type) {
	case *errors.UnmarshalTypeError:
		e.Struct = d.structName
		e.Field = d.fieldName
	case *errors.SyntaxError:
		e.Offset = cursor
	}
}

func (d *unmarshalJSONDecoder) DecodeArray(arr arrow.Array, i int, p unsafe.Pointer) error {
	if arr.IsNull(i) {
		return nil
	}

	v := *(*interface{})(unsafe.Pointer(&emptyInterface{
		typ: d.typ,
		ptr: p,
	}))
	switch v := v.(type) {
	case *time.Time:
		if ts, ok := arr.(*array.Timestamp); ok {
			t := ts.Value(i).ToTime(ts.DataType().(*arrow.TimestampType).Unit)
			if l, err := time.LoadLocation(ts.DataType().(*arrow.TimestampType).TimeZone); err == nil {
				*v = t.In(l)
			} else {
				*v = t
			}
		} else if td, ok := arr.(*array.Date32); ok {
			*v = td.Value(i).ToTime()
		}
	case json.Unmarshaler:
		if arr, ok := arr.(*array.String); ok {
			if err := v.UnmarshalJSON([]byte(arr.Value(i))); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *unmarshalJSONDecoder) Decode(ctx *RuntimeContext, cursor, depth int64, p unsafe.Pointer) (int64, error) {
	buf := ctx.Buf
	cursor = skipWhiteSpace(buf, cursor)
	start := cursor
	end, err := skipValue(buf, cursor, depth)
	if err != nil {
		return 0, err
	}
	src := buf[start:end]
	dst := make([]byte, len(src))
	copy(dst, src)

	v := *(*interface{})(unsafe.Pointer(&emptyInterface{
		typ: d.typ,
		ptr: p,
	}))
	if (ctx.Option.Flags & ContextOption) != 0 {
		if err := v.(unmarshalerContext).UnmarshalJSON(ctx.Option.Context, dst); err != nil {
			d.annotateError(cursor, err)
			return 0, err
		}
	} else {
		if err := v.(json.Unmarshaler).UnmarshalJSON(dst); err != nil {
			d.annotateError(cursor, err)
			return 0, err
		}
	}
	return end, nil
}

func (d *unmarshalJSONDecoder) DecodePath(ctx *RuntimeContext, cursor, depth int64) ([][]byte, int64, error) {
	return nil, 0, fmt.Errorf("json: unmarshal json decoder does not support decode path")
}

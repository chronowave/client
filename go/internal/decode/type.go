package decode

import (
	"context"
	"encoding"
	"encoding/json"
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow"
)

type Decoder interface {
	Decode(*RuntimeContext, int64, int64, unsafe.Pointer) (int64, error)
	DecodePath(*RuntimeContext, int64, int64) ([][]byte, int64, error)
	DecodeArray(arrow.Array, int, unsafe.Pointer) error
}

const (
	nul                   = '\000'
	maxDecodeNestingDepth = 10000
)

type unmarshalerContext interface {
	UnmarshalJSON(context.Context, []byte) error
}

var (
	unmarshalJSONType        = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
	unmarshalJSONContextType = reflect.TypeOf((*unmarshalerContext)(nil)).Elem()
	unmarshalTextType        = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
)

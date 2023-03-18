package decode

import (
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"

	"github.com/chronowave/client/go/internal/errors"
	"github.com/chronowave/client/go/internal/runtime"
)

func Unmarshal(record arrow.Record, v interface{}) error {
	header := (*emptyInterface)(unsafe.Pointer(&v))

	if err := validateType(header.typ, uintptr(header.ptr)); err != nil {
		return err
	}

	dec, err := CompileToGetDecoder(header.typ)
	if err != nil {
		return err
	}

	sliceDec, ok := dec.(*sliceDecoder)
	if !ok {
		return &errors.InvalidUnmarshalError{Type: runtime.RType2Type(header.typ)}
	}

	arr := array.RecordToStructArray(record)
	defer arr.Release()
	return sliceDec.DecodeStructArray(arr, header.ptr)
}

func validateType(typ *runtime.Type, p uintptr) error {
	if typ == nil || typ.Kind() != reflect.Ptr || p == 0 {
		return &errors.InvalidUnmarshalError{Type: runtime.RType2Type(typ)}
	}
	return nil
}

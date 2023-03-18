package client

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow/go/v10/arrow"

	"github.com/chronowave/client/go/internal/decode"
	"github.com/chronowave/fbs/go"
)

type DateFormat struct {
	Is32Bits bool
	TimeUnit arrow.TimeUnit
	TimeZone string
	Layout   string
}

func EmptyDateFormat() map[string]DateFormat {
	return make(map[string]DateFormat)
}

var marshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()

func UnmarshalRecord(record arrow.Record, v any) error {
	return decode.Unmarshal(record, v)
}

func DeriveArrowSchema(obj any, format map[string]DateFormat) (*arrow.Schema, error) {
	if format == nil {
		format = EmptyDateFormat()
	} else {
		for n, f := range format {
			if len(f.TimeZone) > 0 {
				if _, err := time.LoadLocation(f.TimeZone); err != nil {
					return nil, fmt.Errorf("field %s has invalid timezone %s", n, f.TimeZone)
				}
			}
		}
	}

	base := reflect.TypeOf(obj)
	if base.Kind() == reflect.Pointer {
		base = base.Elem()
	}

	if base.Kind() != reflect.Struct {
		return nil, fmt.Errorf("support struct only")
	}

	n := base.NumField()

	fields := make([]arrow.Field, 0, n)
	for i := 0; i < n; i++ {
		if field, ok := toArrowField(base.Field(i), format); ok {
			fields = append(fields, field)
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func toArrowField(sf reflect.StructField, format map[string]DateFormat) (arrow.Field, bool) {
	tag := sf.Tag.Get("json")
	if tag == "-" {
		return arrow.Field{}, false
	}

	name, _ := parseTag(tag)
	if !isValidTag(name) {
		name = ""
	}

	arrowType, metadata := toArrowDataType(sf.Type, name, format)

	return arrow.Field{
		Name:     name,
		Type:     arrowType,
		Nullable: true,
		Metadata: metadata,
	}, true
}

func toArrowStructType(t reflect.Type, format map[string]DateFormat) arrow.DataType {
	n := t.NumField()
	fields := make([]arrow.Field, 0, n)
	for i := 0; i < n; i++ {
		if f, ok := toArrowField(t.Field(i), format); ok {
			fields = append(fields, f)
		}
	}

	return arrow.StructOf(fields...)
}

func toArrowArrayType(t reflect.Type, name string, format map[string]DateFormat) (arrow.DataType, arrow.Metadata) {
	arrowType, metadata := toArrowDataType(t.Elem(), name, format)
	return arrow.ListOfField(arrow.Field{Type: arrowType, Nullable: true}), metadata
}

func toArrowDataType(base reflect.Type, name string, format map[string]DateFormat) (arrow.DataType, arrow.Metadata) {
	if base.Kind() == reflect.Pointer {
		base = base.Elem()
	}

	var arrowType arrow.DataType
	metadata := arrow.Metadata{}
	if base.Implements(marshalerType) {
		if base == reflect.TypeOf(time.Time{}) {
			layout := time.RFC3339Nano
			if tf, ok := format[name]; ok {
				if tf.Is32Bits {
					arrowType = &arrow.Date32Type{}
				} else {
					arrowType = &arrow.TimestampType{
						Unit:     tf.TimeUnit,
						TimeZone: tf.TimeZone,
					}
				}

				if len(tf.Layout) > 0 {
					layout = tf.Layout
				}
			} else {
				// default is
				arrowType = &arrow.TimestampType{Unit: arrow.Millisecond}
			}
			metadata = arrow.NewMetadata([]string{fbs.EnumNamesMetadataKey[fbs.MetadataKeyLAYOUT]}, []string{layout})
		} else {
			// it will convert to string
			arrowType = &arrow.StringType{}
		}
	} else {
		// NOTE: doesn't support uint, uint will be equivalent int type
		switch base.Kind() {
		case reflect.Bool:
			arrowType = &arrow.BooleanType{}
		case reflect.Int:
			arrowType = &arrow.Int32Type{}
		case reflect.Int8:
			arrowType = &arrow.Int8Type{}
		case reflect.Int16:
			arrowType = &arrow.Int16Type{}
		case reflect.Int32:
			arrowType = &arrow.Int32Type{}
		case reflect.Int64:
			arrowType = &arrow.Int64Type{}
		case reflect.Uint:
			arrowType = &arrow.Int32Type{}
		case reflect.Uint8:
			arrowType = &arrow.Int8Type{}
		case reflect.Uint16:
			arrowType = &arrow.Int16Type{}
		case reflect.Uint32:
			arrowType = &arrow.Int32Type{}
		case reflect.Uint64:
			arrowType = &arrow.Int64Type{}
		case reflect.Uintptr:
			arrowType = &arrow.Int32Type{}
		case reflect.Float32:
			arrowType = &arrow.Float32Type{}
		case reflect.Float64:
			arrowType = &arrow.Float64Type{}
		case reflect.Array:
			arrowType, metadata = toArrowArrayType(base, name, format)
		case reflect.Map:
			arrowType = toArrowStructType(base, format)
		case reflect.Slice:
			arrowType, metadata = toArrowArrayType(base, name, format)
		case reflect.String:
			arrowType = &arrow.StringType{}
		case reflect.Struct:
			arrowType = toArrowStructType(base, format)
		default:
			panic(fmt.Sprintf("unsupported field type %v: %v ", name, base))
		}
	}

	return arrowType, metadata
}

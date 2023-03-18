package client

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
)

func TestDeriveArrowSchema(t *testing.T) {
	span := struct {
		Span string `json:"span_id"`
	}{}

	schema, err := DeriveArrowSchema(&span, nil)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
		return
	}
	want := []arrow.Field{{Name: "span_id", Type: &arrow.StringType{}, Nullable: true}}
	if !reflect.DeepEqual(want, schema.Fields()) {
		t.Errorf("want=%v, got=%v", want, schema.Fields())
	}
}

func TestUnmarshallRecord(t *testing.T) {
	dt := arrow.ListOfField(arrow.Field{
		Type: &arrow.TimestampType{
			Unit:     arrow.Microsecond,
			TimeZone: "America/New_York",
		},
		Nullable: true,
	})

	fields := []arrow.Field{{Name: "a", Type: dt, Nullable: true}}

	times := []time.Time{
		time.Now().Add(-1 * time.Hour),
		time.Now().Add(-30 * time.Minute),
		time.Now(),
	}

	structBuilder := array.NewStructBuilder(memory.DefaultAllocator, arrow.StructOf(fields...))
	list := structBuilder.FieldBuilder(0).(*array.ListBuilder)
	builder := list.ValueBuilder().(*array.TimestampBuilder)
	for _, t := range times {
		list.Append(true)
		builder.Append(arrow.Timestamp(t.UnixMicro()))
	}

	schema := arrow.NewSchema(fields, nil)
	record := array.NewRecord(schema, []arrow.Array{list.NewArray()}, int64(len(times)))

	data, _ := record.MarshalJSON()
	fmt.Printf("%v", string(data))

	var got []struct{ A [1]time.Time }
	_ = UnmarshalRecord(record, &got)

	if len(got) != len(times) {
		t.Errorf("record length doesn't match want=%v, got=%v", len(got), len(times))
		return
	}

	for i, v := range got {
		// milliseconds precision
		if !reflect.DeepEqual(v.A[0].UnixMilli(), times[i].UnixMilli()) {
			t.Errorf("doesn't equal want=%v, got=%v", times[i], v.A[0])
		}
	}
}

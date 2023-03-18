package decode

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"

	"github.com/chronowave/client/go/internal/errors"
	"github.com/chronowave/client/go/internal/runtime"
)

type arrayDecoder struct {
	elemType     *runtime.Type
	size         uintptr
	valueDecoder Decoder
	alen         int
	structName   string
	fieldName    string
	zeroValue    unsafe.Pointer
}

func newArrayDecoder(dec Decoder, elemType *runtime.Type, alen int, structName, fieldName string) *arrayDecoder {
	// workaround to avoid checkptr errors. cannot use `*(*unsafe.Pointer)(unsafe_New(elemType))` directly.
	zeroValuePtr := unsafe_New(elemType)
	zeroValue := **(**unsafe.Pointer)(unsafe.Pointer(&zeroValuePtr))
	return &arrayDecoder{
		valueDecoder: dec,
		elemType:     elemType,
		size:         elemType.Size(),
		alen:         alen,
		structName:   structName,
		fieldName:    fieldName,
		zeroValue:    zeroValue,
	}
}

func (d *arrayDecoder) DecodeArray(s arrow.Array, i int, p unsafe.Pointer) error {
	if s.IsNull(i) {
		for idx := 0; idx < d.alen; idx++ {
			*(*unsafe.Pointer)(unsafe.Pointer(uintptr(p) + uintptr(idx)*d.size)) = d.zeroValue
		}
	} else {
		list := s.(*array.List)
		start, end := list.ValueOffsets(i)
		if d.alen != int(end-start) {
			return fmt.Errorf("array length is not equal")
		}
		values := list.ListValues()
		for idx := 0; idx < d.alen; idx++ {
			if err := d.valueDecoder.DecodeArray(values, int(start)+idx, unsafe.Pointer(uintptr(p)+uintptr(idx)*d.size)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *arrayDecoder) Decode(ctx *RuntimeContext, cursor, depth int64, p unsafe.Pointer) (int64, error) {
	buf := ctx.Buf
	depth++
	if depth > maxDecodeNestingDepth {
		return 0, errors.ErrExceededMaxDepth(buf[cursor], cursor)
	}

	for {
		switch buf[cursor] {
		case ' ', '\n', '\t', '\r':
			cursor++
			continue
		case 'n':
			if err := validateNull(buf, cursor); err != nil {
				return 0, err
			}
			cursor += 4
			return cursor, nil
		case '[':
			idx := 0
			cursor++
			cursor = skipWhiteSpace(buf, cursor)
			if buf[cursor] == ']' {
				for idx < d.alen {
					*(*unsafe.Pointer)(unsafe.Pointer(uintptr(p) + uintptr(idx)*d.size)) = d.zeroValue
					idx++
				}
				cursor++
				return cursor, nil
			}
			for {
				if idx < d.alen {
					c, err := d.valueDecoder.Decode(ctx, cursor, depth, unsafe.Pointer(uintptr(p)+uintptr(idx)*d.size))
					if err != nil {
						return 0, err
					}
					cursor = c
				} else {
					c, err := skipValue(buf, cursor, depth)
					if err != nil {
						return 0, err
					}
					cursor = c
				}
				idx++
				cursor = skipWhiteSpace(buf, cursor)
				switch buf[cursor] {
				case ']':
					for idx < d.alen {
						*(*unsafe.Pointer)(unsafe.Pointer(uintptr(p) + uintptr(idx)*d.size)) = d.zeroValue
						idx++
					}
					cursor++
					return cursor, nil
				case ',':
					cursor++
					continue
				default:
					return 0, errors.ErrInvalidCharacter(buf[cursor], "array", cursor)
				}
			}
		default:
			return 0, errors.ErrUnexpectedEndOfJSON("array", cursor)
		}
	}
}

func (d *arrayDecoder) DecodePath(ctx *RuntimeContext, cursor, depth int64) ([][]byte, int64, error) {
	return nil, 0, fmt.Errorf("json: array decoder does not support decode path")
}

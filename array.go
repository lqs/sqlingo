package sqlingo

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unicode"
)

type ArrayDimension struct {
	Length     int32
	LowerBound int32
}

type untypedTextArray struct {
	Elements   []string         // elements as string
	Quoted     []bool           // true if source is quoted
	Dimensions []ArrayDimension //
}

// Skip the space prefix
func skipWhitespace(buf *bytes.Buffer) {
	var r rune
	var err error
	for r, _, _ = buf.ReadRune(); unicode.IsSpace(r); r, _, _ = buf.ReadRune() {
	}

	if err != io.EOF {
		buf.UnreadRune()
	}
}

func parseToUntypedTextArray(src string) (*untypedTextArray, error) {
	dst := &untypedTextArray{
		Elements:   []string{},
		Quoted:     []bool{},
		Dimensions: []ArrayDimension{},
	}

	buf := bytes.NewBufferString(src)
	skipWhitespace(buf)

	// read failed
	r, _, err := buf.ReadRune()
	if err != nil {
		return nil, fmt.Errorf("invalid array: %v", err)
	}

	var explicitDimensions []ArrayDimension

	// Array has explicit dimensions
	if r == '[' {
		buf.UnreadRune()
		for {
			r, _, err = buf.ReadRune()
			if err != nil {
				return nil, fmt.Errorf("invalid array: %v", err)
			}
			if r == '=' {
				break
			} else if r != '[' {
				return nil, fmt.Errorf("invalid array, expected '[' or '=' got %v", r)
			}

			lower, err := arrayParseInteger(buf)
			if err != nil {
				return nil, fmt.Errorf("invalid array: %v", err)
			}

			r, _, err = buf.ReadRune()
			if err != nil {
				return nil, fmt.Errorf("invalid array: %v", err)
			}

			if r != ':' {
				return nil, fmt.Errorf("invalid array, expected ':' got %v", r)
			}

			upper, err := arrayParseInteger(buf)
			if err != nil {
				return nil, fmt.Errorf("invalid array: %v", err)
			}

			r, _, err = buf.ReadRune()
			if err != nil {
				return nil, fmt.Errorf("invalid array: %v", err)
			}

			if r != ']' {
				return nil, fmt.Errorf("invalid array, expected ']' got %v", r)
			}

			explicitDimensions = append(explicitDimensions, ArrayDimension{LowerBound: lower, Length: upper - lower + 1})
		}
	}

	if r != '{' {
		return nil, errors.New("invalid array, expected '{' prefix")
	}
	implicitDimensions := []ArrayDimension{{LowerBound: 1, Length: 0}}

	// Consume all initial opening brackets. This provides number of dimensions.
	for {
		r, _, err = buf.ReadRune()
		if err != nil {
			return nil, fmt.Errorf("invalid array: %v", err)
		}

		if r == '{' {
			implicitDimensions[len(implicitDimensions)-1].Length = 1
			implicitDimensions = append(implicitDimensions, ArrayDimension{LowerBound: 1})
		} else {
			buf.UnreadRune()
			break
		}
	}
	currentDim := len(implicitDimensions) - 1
	counterDim := currentDim

	for {
		r, _, err = buf.ReadRune()
		if err != nil {
			return nil, fmt.Errorf("invalid array: %v", err)
		}

		switch r {
		case '{':
			if currentDim == counterDim {
				implicitDimensions[currentDim].Length++
			}
			currentDim++
		case ',':
		case '}':
			currentDim--
			if currentDim < counterDim {
				counterDim = currentDim
			}
		default:
			buf.UnreadRune()
			value, quoted, err := arrayParseValue(buf)
			if err != nil {
				return nil, fmt.Errorf("invalid array value: %v", err)
			}
			if currentDim == counterDim {
				implicitDimensions[currentDim].Length++
			}
			dst.Quoted = append(dst.Quoted, quoted)
			dst.Elements = append(dst.Elements, value)
		}

		if currentDim < 0 {
			break
		}
	}

	skipWhitespace(buf)

	if buf.Len() > 0 {
		return nil, fmt.Errorf("unexpected trailing data: %v", buf.String())
	}

	if len(dst.Elements) == 0 {
	} else if len(explicitDimensions) > 0 {
		dst.Dimensions = explicitDimensions
	} else {
		dst.Dimensions = implicitDimensions
	}

	return dst, nil
}

func arrayParseInteger(buf *bytes.Buffer) (int32, error) {
	s := &bytes.Buffer{}

	for {
		r, _, err := buf.ReadRune()
		if err != nil {
			return 0, err
		}

		if ('0' <= r && r <= '9') || r == '-' {
			s.WriteRune(r)
		} else {
			buf.UnreadRune()
			n, err := strconv.ParseInt(s.String(), 10, 32)
			if err != nil {
				return 0, err
			}
			return int32(n), nil
		}
	}
}
func arrayParseValue(buf *bytes.Buffer) (string, bool, error) {
	r, _, err := buf.ReadRune()
	if err != nil {
		return "", false, err
	}
	if r == '"' {
		return arrayParseQuotedValue(buf)
	}
	buf.UnreadRune()

	s := &bytes.Buffer{}

	for {
		r, _, err := buf.ReadRune()
		if err != nil {
			return "", false, err
		}

		switch r {
		case ',', '}':
			buf.UnreadRune()
			return s.String(), false, nil
		}

		s.WriteRune(r)
	}
}

func arrayParseQuotedValue(buf *bytes.Buffer) (string, bool, error) {
	s := &bytes.Buffer{}

	for {
		r, _, err := buf.ReadRune()
		if err != nil {
			return "", false, err
		}

		switch r {
		case '\\':
			r, _, err = buf.ReadRune()
			if err != nil {
				return "", false, err
			}
		case '"':
			r, _, err = buf.ReadRune()
			if err != nil {
				return "", false, err
			}
			buf.UnreadRune()
			return s.String(), true, nil
		}
		s.WriteRune(r)
	}
}

package internal

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

// Struct the capture the input received
type Value struct {
	Typ   string  // hold the type of data
	Str   string  // hold string value
	Num   int     // hold number value
	Bulk  string  // if type is bulk hold the bulk string
	Array []Value // if type is array hold the array
}
type Resp struct {
	reader *bufio.Reader
}

func NewResp(reader io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(reader)}
}

// interpret Command
func (r *Resp) ProcessCMD() (Value, error) {
	op, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch op {
	case ARRAY:
		// slog.Info("Array Input")
		return r.readArray()
	case BULK:
		// slog.Info("Bulk String Input")
		return r.readBulk()
	default:
		slog.Info("None")
		return Value{}, fmt.Errorf("invalid command")
	}
}

// *2\r\n$3\r\nGETr\n$5r\nShyam
// 2\r\n$3\r\nGETr\n$5r\nShyam
// $3\r\nGET\r\n$5\r\nShyam
func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.Typ = "array"
	num, _ := r.readInt()

	v.Array = make([]Value, 0)
	for i := 0; i < num; i++ {
		val, _ := r.ProcessCMD()
		v.Array = append(v.Array, val)
	}

	return v, nil
}

// *3\r\n$3\r\nset\r\n$5\r\nadmin\r\n$5\r\nahmed
// *2\r\n$3\r\nGETr\n$5r\nShyam
// *6\r\n$5\r\nHMSET\r\n$5\r\nuser1\r\n$4\r\nname\r\n$5\r\nshyam\r\n$3\r\nage\r\n$2\r\n10

// function to read a bulk string
func (r *Resp) readBulk() (Value, error) {
	v := Value{}
	v.Typ = "bulk"

	num, _ := r.readInt()

	buff := make([]byte, num)
	r.reader.Read(buff)
	v.Bulk = string(buff)

	// this will read and remove trailing \r\n eg GETr\n
	r.readLine()
	return v, nil
}

func (r *Resp) readInt() (int, error) {
	data, err := r.readLine()
	if err != nil {
		return 0, err
	}

	count, _ := strconv.ParseInt(string(data), 10, 64)

	return int(count), nil
}

// *3\r\n$3\r\nset\r\n$5\r\nadmin\r\n$5\r\nahmed
func (r *Resp) readLine() (line []byte, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, err
		}

		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}

	}
	return line[:len(line)-2], nil
}

func (v Value) Marshal() []byte {
	switch v.Typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshallNull()
	case "error":
		return v.marshallError()
	default:
		return []byte{}
	}
}

// This converts back to RESP, used for aof storage
func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.Bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.Bulk...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshalArray() []byte {
	len := len(v.Array)
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.Array[i].Marshal()...)
	}

	return bytes
}

func (v Value) marshallError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()
	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}

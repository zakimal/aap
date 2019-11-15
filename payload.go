package aap

import (
	"bytes"
	"encoding/binary"
	"github.com/pkg/errors"
	"io"
	"math"
)

type Reader struct {
	reader *bytes.Reader
}

func NewReader(payload []byte) Reader {
	return Reader{
		reader: bytes.NewReader(payload),
	}
}

func (r Reader) Len() int {
	return r.reader.Len()
}

func (r Reader) Read(buf []byte) (n int, err error) {
	return r.reader.Read(buf)
}

func (r Reader) ReadBytes() ([]byte, error) {
	raw, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}
	size := int(raw)
	if size < 0 || size > r.reader.Len() {
		return nil, errors.New("bytes out of bounds")
	}
	buf := make([]byte, size)
	_, err = r.Read(buf)
	if err != nil && errors.Cause(err) != io.EOF {
		return nil, err
	}
	return buf, nil
}

func (r Reader) ReadString() (string, error) {
	b, err := r.ReadBytes()
	return string(b), err
}

func (r Reader) ReadByte() (byte, error) {
	return r.reader.ReadByte()
}

func (r Reader) ReadUint16() (uint16, error) {
	var buf [2]byte
	_, err := r.reader.Read(buf[:])
	return binary.LittleEndian.Uint16(buf[:]), err
}

func (r Reader) ReadUint32() (uint32, error) {
	var buf [4]byte
	_, err := r.reader.Read(buf[:])
	return binary.LittleEndian.Uint32(buf[:]), err
}

func (r Reader) ReadUint64() (uint64, error) {
	var buf [8]byte
	_, err := r.reader.Read(buf[:])
	return binary.LittleEndian.Uint64(buf[:]), err
}

func (r Reader) ReadFloat32() (float32, error) {
	var buf [4]byte
	_, err := r.reader.Read(buf[:])
	bits := binary.LittleEndian.Uint32(buf[:])
	float := math.Float32frombits(bits)
	return float, err
}

func (r Reader) ReadFloat64() (float64, error) {
	var buf [8]byte
	_, err := r.reader.Read(buf[:])
	bits := binary.LittleEndian.Uint64(buf[:])
	float := math.Float64frombits(bits)
	return float, err
}

type Writer struct {
	buffer *bytes.Buffer
}

func NewWriter(buf []byte) Writer {
	return Writer{
		buffer: bytes.NewBuffer(buf),
	}
}

func (w Writer) Len() int {
	return w.buffer.Len()
}

func (w Writer) Write(buf []byte) (n int, err error) {
	return w.buffer.Write(buf)
}

func (w Writer) Bytes() []byte {
	return w.buffer.Bytes()
}

func (w Writer) WriteBytes(buf []byte) Writer {
	w.WriteUint32(uint32(len(buf)))
	_, _ = w.Write(buf)
	return w
}

func (w Writer) WriteString(s string) Writer {
	w.WriteBytes([]byte(s))
	return w
}

func (w Writer) WriteByte(b byte) Writer {
	w.buffer.WriteByte(b)
	return w
}

func (w Writer) WriteUint16(u16 uint16) Writer {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], u16)
	_, _ = w.Write(buf[:])
	return w
}

func (w Writer) WriteUint32(u32 uint32) Writer {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], u32)
	_, _ = w.Write(buf[:])
	return w
}

func (w Writer) WriteUint64(u64 uint64) Writer {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], u64)
	_, _ = w.Write(buf[:])
	return w
}

func (w Writer) WriteFloat32(f32 float32) Writer {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], math.Float32bits(f32))
	_, _ = w.Write(buf[:])
	return w
}

func (w Writer) WriteFloat64(f64 float64) Writer {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(f64))
	_, _ = w.Write(buf[:])
	return w
}

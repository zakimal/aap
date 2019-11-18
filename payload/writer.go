package payload

import (
	"bytes"
	"encoding/binary"
	"math"
)

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

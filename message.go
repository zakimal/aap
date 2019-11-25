package aap

import (
	"github.com/pkg/errors"
	"github.com/zakimal/aap/payload"
)

type Message interface {
	Read(reader payload.Reader) (Message, error)
	Write() []byte
}

type MessageNil struct{}

func (MessageNil) Read(reader payload.Reader) (Message, error) { return MessageNil{}, nil }
func (MessageNil) Write() []byte                               { return nil }

type MessageHello struct {
	from uint64
}

func (MessageHello) Read(reader payload.Reader) (Message, error) {
	from, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `from` of `HelloMessage`")
	}
	return MessageHello{from: from}, nil
}
func (m MessageHello) Write() []byte {
	return payload.NewWriter(nil).WriteUint64(m.from).Bytes()
}

type MessagePEvalRequest struct{
	from uint64
}

func (MessagePEvalRequest) Read(reader payload.Reader) (Message, error) {
	from, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `from` of `MessagePEvalRequest`")
	}
	return MessagePEvalRequest{from: from}, nil
}
func (m MessagePEvalRequest) Write() []byte {
	return payload.NewWriter(nil).WriteUint64(m.from).Bytes()
}

type MessagePEvalResponse struct{
	from uint64
	debugText string
}

func (MessagePEvalResponse) Read(reader payload.Reader) (Message, error) {
	from, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `from` of `MessagePEvalResponse`")
	}
	debugText, err := reader.ReadString()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `debugText` of `MessagePEvalResponse`")
	}
	return MessagePEvalResponse{from: from, debugText:debugText}, nil
}
func (m MessagePEvalResponse) Write() []byte {
	return payload.NewWriter(nil).WriteUint64(m.from).WriteString(m.debugText).Bytes()
}

type MessageIncEvalUpdate struct{
	from uint64
	round uint64
	nid int64
	data float64
}

func (MessageIncEvalUpdate) Read(reader payload.Reader) (Message, error) {
	from, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `from` of `MessageIncEvalUpdate`")
	}
	round, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `round` of `MessageIncEvalUpdate`")
	}
	nid, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `nid` of `MessageIncEvalUpdate`")
	}
	data, err := reader.ReadFloat64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `data` of `MessageIncEvalUpdate`")
	}
	return MessageIncEvalUpdate{
		from:  from,
		round: round,
		nid:   int64(nid),
		data:  data,
	}, nil
}
func (m MessageIncEvalUpdate) Write() []byte {
	return payload.NewWriter(nil).
		WriteUint64(m.from).
		WriteUint64(m.round).
		WriteUint64(uint64(m.nid)).
		WriteFloat64(m.data).
		Bytes()
}

type MessageNotifyInactive struct{
	from uint64
}

func (MessageNotifyInactive) Read(reader payload.Reader) (Message, error) {
	from, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `from` of `MessageNotifyInactive`")
	}
	return MessageNotifyInactive{from:from}, nil
}
func (m MessageNotifyInactive) Write() []byte {
	return payload.NewWriter(nil).WriteUint64(m.from).Bytes()
}

type MessageTerminate struct{
	from uint64
	debugText string
}

func (MessageTerminate) Read(reader payload.Reader) (Message, error) {
	from, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `from` of `MessageTerminate`")
	}
	debugText, err := reader.ReadString()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `debugText` of `MessageTerminate`")
	}
	return MessageTerminate{from: from, debugText:debugText}, nil
}
func (m MessageTerminate) Write() []byte {
	return payload.NewWriter(nil).WriteUint64(m.from).WriteString(m.debugText).Bytes()
}

type MessageTerminateACK struct{
	from uint64
}

func (MessageTerminateACK) Read(reader payload.Reader) (Message, error) {
	from, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `from` of `MessageTerminateACK`")
	}
	return MessageTerminateACK{from:from}, nil
}
func (m MessageTerminateACK) Write() []byte {
	return payload.NewWriter(nil).WriteUint64(m.from).Bytes()
}

type MessageAssembleRequest struct{
	from uint64
	debugText string
}

func (MessageAssembleRequest) Read(reader payload.Reader) (Message, error) {
	from, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `from` of `MessageAssembleRequest`")
	}
	debugText, err := reader.ReadString()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `debugText` of `MessageAssembleRequest`")
	}
	return MessageAssembleRequest{from: from, debugText:debugText}, nil
}
func (m MessageAssembleRequest) Write() []byte {
	return payload.NewWriter(nil).WriteUint64(m.from).WriteString(m.debugText).Bytes()
}

type MessageAssembleResponse struct{
	from uint64
	result map[int64]float64
}

func (m MessageAssembleResponse) Read(reader payload.Reader) (Message, error) {
	from, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `from` of `MessageAssembleResponse`")
	}
	length, err := reader.ReadUint64()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read `len(result)` of `MessageAssembleResponse`")
	}
	result := make(map[int64]float64)
	var i uint64
	for i = 0; i < length; i++ {
		nid, err := reader.ReadUint64()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read %d th key of `result` of `MessageAssembleResponse`", i)
		}
		data, err := reader.ReadFloat64()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read %d th value of `result` of `MessageAssembleResponse`", i)
		}
		m.result[int64(nid)] = data
	}
	return MessageAssembleResponse{
		from:   from,
		result: result,
	}, nil
}
func (m MessageAssembleResponse) Write() []byte {
	writer := payload.NewWriter(nil)
	writer.WriteUint64(m.from)
	writer.WriteUint64(uint64(len(m.result)))
	for nid, data := range m.result {
		writer.WriteUint64(uint64(nid))
		writer.WriteFloat64(data)
	}
	return writer.Bytes()
}
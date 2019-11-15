package aap

import (
	"github.com/pkg/errors"
)

type Message interface {
	Read(reader Reader) (Message, error)
	Write() []byte
}

type MessageNil struct{}

func (MessageNil) Read(reader Reader) (Message, error) { return MessageNil{}, nil }
func (MessageNil) Write() []byte                               { return nil }

type ChatMessage struct {
	Text string
}

func (ChatMessage) Read(reader Reader) (Message, error) {
	text, err := reader.ReadString()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read chat msg")
	}

	return ChatMessage{Text: text}, nil
}

func (m ChatMessage) Write() []byte {
	return NewWriter(nil).WriteString(m.Text).Bytes()
}
package aap

import (
	"github.com/zakimal/aap/payload"
)

type Message interface {
	Read(reader payload.Reader) (Message, error)
	Write() []byte
}

type MessageNil struct{}
func (MessageNil) Read(reader payload.Reader) (Message, error) { return MessageNil{}, nil }
func (MessageNil) Write() []byte                               { return nil }

package aap

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/zakimal/aap/log"
	"reflect"
	"sync"
)

type Opcode byte

const OpcodeNil Opcode = 0

var (
	opcodes  map[Opcode]Message
	messages map[reflect.Type]Opcode
	mutex    sync.Mutex
)

func (o Opcode) Byte() [1]byte {
	var b [1]byte
	b[0] = byte(o)
	return b
}

func NextAvailableOpcode() Opcode {
	mutex.Lock()
	defer mutex.Unlock()
	return Opcode(len(opcodes))
}

func DebugOpcodes() {
	mutex.Lock()
	defer mutex.Unlock()
	log.Debug().Msg("[DEBUG] Here are all opcodes registered so far")
	for op, msg := range opcodes {
		fmt.Printf("\t[%d] <==> %s\n", op, reflect.TypeOf(msg).String())
	}
}

func MessageFromOpcode(op Opcode) (Message, error) {
	mutex.Lock()
	defer mutex.Unlock()
	typ, exist := opcodes[op]
	if !exist {
		return nil, errors.Errorf("There is no messageReceiver type registered to opcode [%d]\n", op)
	}
	msg, ok := reflect.New(reflect.TypeOf(typ)).Elem().Interface().(Message)
	if !ok {
		return nil, errors.Errorf("Invalid messageReceiver type associated to opcode [%d]\n", op)
	}
	return msg, nil
}

func OpcodeFromMessage(msg Message) (Opcode, error) {
	mutex.Lock()
	defer mutex.Unlock()
	typ := reflect.TypeOf(msg)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	op, exist := messages[typ]
	if !exist {
		return OpcodeNil, errors.Errorf("There is no opcode registered for messageReceiver type %v\n", typ)
	}
	return op, nil
}

func RegisterMessage(op Opcode, msg interface{}) Opcode {
	typ := reflect.TypeOf(msg).Elem()
	mutex.Lock()
	defer mutex.Unlock()
	if opcode, registered := messages[typ]; registered {
		return opcode
	}
	opcodes[op] = reflect.New(typ).Elem().Interface().(Message)
	messages[typ] = op
	return op
}

func resetOpcodes() {
	mutex.Lock()
	defer mutex.Unlock()
	opcodes = map[Opcode]Message{
		OpcodeNil: reflect.New(reflect.TypeOf((*MessageNil)(nil)).Elem()).Elem().Interface().(Message),
	}
	messages = map[reflect.Type]Opcode{
		reflect.TypeOf((*MessageNil)(nil)).Elem(): OpcodeNil,
	}
}

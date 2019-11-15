package aap

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/zakimal/aap/log"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

type ReceiveHandler struct {
	hub chan Message
	lock chan struct{}
}

func (r *ReceiveHandler) Unlock() {
	<- r.lock
}

type SendHandler struct {
	payload []byte
	result chan error
}

type Peer struct {
	worker *Worker
	conn net.Conn
	sendQueue chan SendHandler
	recvQueue sync.Map
	kill chan *sync.WaitGroup
	killOnce uint32
}

func NewPeer(worker *Worker, conn net.Conn) *Peer {
	return &Peer{
		worker:    worker,
		conn:      conn,
		sendQueue: make(chan SendHandler, 128),
		kill:      make(chan *sync.WaitGroup),
		killOnce:  0,
	}
}
func (p *Peer) init() {
	go p.spawnMessageReceiver()
	go p.spawnMessageSender()
}
func (p *Peer) spawnMessageSender() {
	for {
		var cmd SendHandler
		select {
		case wg := <-p.kill:
			wg.Done()
			return
		case cmd = <-p.sendQueue:
		}
		payload := cmd.payload
		size := len(payload)
		buf := make([]byte, binary.MaxVarintLen64)
		prepend := binary.PutUvarint(buf[:], uint64(size))
		buf = append(buf[:prepend], payload[:]...)
		copied, err := io.Copy(p.conn, bytes.NewReader(buf))
		if copied != int64(size+prepend) {
			if cmd.result != nil {
				cmd.result <- errors.Errorf(
					"only written %d bytes when expected to write %d bytes to setupTestPeer\n",
					copied, size+prepend)
				close(cmd.result)
			}
			continue
		}
		if err != nil {
			if cmd.result != nil {
				cmd.result <- errors.Wrap(err, "failed to send message to setupTestPeer")
				close(cmd.result)
			}
			continue
		}
		if cmd.result != nil {
			cmd.result <- nil
			close(cmd.result)
		}
	}
}
func (p *Peer) spawnMessageReceiver() {
	reader := bufio.NewReader(p.conn)
	for {
		select {
		case wg := <-p.kill:
			wg.Done()
			return
		default:
		}
		size, err := binary.ReadUvarint(reader)
		if err != nil {
			p.DisconnectAsync()
			continue
		}
		buf := make([]byte, int(size))
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			p.DisconnectAsync()
			continue
		}
		opcode, msg, err := p.DecodeMessage(buf)
		if opcode == OpcodeNil || err != nil {
			p.DisconnectAsync()
			continue
		}
		c, _ := p.recvQueue.LoadOrStore(opcode, ReceiveHandler{hub: make(chan Message), lock: make(chan struct{}, 1)})
		recv := c.(ReceiveHandler)
		select {
		case recv.hub <- msg:
			recv.lock <- struct{}{}
			log.Info().Msgf("Recv: %+v", recv.hub)
			<-recv.lock
		}
	}
}
func (p *Peer) SendMessage(message Message) error {
	encodedPayload, err := p.EncodeMessage(message)
	if err != nil {
		return errors.Wrap(err, "failed to serialize message contents to be sent to a setupTestPeer")
	}
	cmd := SendHandler{payload: encodedPayload, result: make(chan error, 1)}
	select {
	case p.sendQueue <- cmd:
	}
	select {
	case err = <-cmd.result:
		return err
	}
}
func (p *Peer) SendMessageAsync(message Message) <-chan error {
	result := make(chan error, 1)
	encodedPayload, err := p.EncodeMessage(message)
	if err != nil {
		result <- errors.Wrap(err, "failed to serialize message contents to be sent to a setupTestPeer")
		return result
	}
	cmd := SendHandler{payload: encodedPayload, result: result}
	select {
	case p.sendQueue <- cmd:
	}
	return result
}
func (p *Peer) Receive(op Opcode) <-chan Message {
	c, _ := p.recvQueue.LoadOrStore(op, ReceiveHandler{hub: make(chan Message), lock: make(chan struct{}, 1)})
	return c.(ReceiveHandler).hub
}
func (p *Peer) Disconnect() {
	if !atomic.CompareAndSwapUint32(&p.killOnce, 0, 1) {
		return
	}
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		p.kill <- &wg
	}
	if err := p.conn.Close(); err != nil {
		panic(err)
	}
	wg.Wait()
	close(p.kill)
}
func (p *Peer) DisconnectAsync() <-chan struct{} {
	signal := make(chan struct{})
	if !atomic.CompareAndSwapUint32(&p.killOnce, 0, 1) {
		close(signal)
		return signal
	}
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		p.kill <- &wg
	}
	if err := p.conn.Close(); err != nil {
		panic(err)
	}
	go func() {
		wg.Wait()
		close(p.kill)
		close(signal)
	}()
	return signal
}
func (p *Peer) LocalAddress() string {
	return fmt.Sprintf("%s:%d", p.LocalIP(), p.LocalPort())
}
func (p *Peer) LocalIP() net.IP {
	return p.worker.transport.IP(p.conn.LocalAddr())
}
func (p *Peer) LocalPort() uint16 {
	return p.worker.transport.Port(p.conn.LocalAddr())
}
func (p *Peer) RemoteAddress() string {
	return fmt.Sprintf("%s:%d", p.RemoteIP(), p.RemotePort())
}
func (p *Peer) RemoteIP() net.IP {
	return p.worker.transport.IP(p.conn.RemoteAddr())
}
func (p *Peer) RemotePort() uint16 {
	return p.worker.transport.Port(p.conn.RemoteAddr())
}
func (p *Peer) Worker() *Worker {
	return p.worker
}
func (p *Peer) LockOnReceive(op Opcode) ReceiveHandler {
	c, _ := p.recvQueue.LoadOrStore(op, ReceiveHandler{hub: make(chan Message), lock: make(chan struct{}, 1)})
	recv := c.(ReceiveHandler)
	recv.lock <- struct{}{}
	return recv
}
func (p *Peer) SetWorker(worker *Worker) {
	p.worker = worker
}
func (p *Peer) EncodeMessage(message Message) ([]byte, error) {
	opcode, err := OpcodeFromMessage(message)
	if err != nil {
		return nil, errors.Wrap(err, "could not find opcode registered for message")
	}
	var buf bytes.Buffer
	_, err = buf.Write(NewWriter(nil).WriteByte(byte(opcode)).Bytes())
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize message opcode")
	}
	_, err = buf.Write(message.Write())
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize and write message contents")
	}
	return buf.Bytes(), nil
}
func (p *Peer) DecodeMessage(buf []byte) (Opcode, Message, error) {
	reader := NewReader(buf)
	opcode, err := reader.ReadByte()
	if err != nil {
		return OpcodeNil, nil, errors.Wrap(err, "failed to read opcode")
	}
	message, err := MessageFromOpcode(Opcode(opcode))
	if err != nil {
		return Opcode(opcode), nil, errors.Wrap(err, "opcode <-> message pairing not registered")
	}
	message, err = message.Read(reader)
	if err != nil {
		return Opcode(opcode), nil, errors.Wrap(err, "failed to read message contents")
	}
	return Opcode(opcode), message, nil
}

package aap

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/zakimal/aap/payload"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

type PSendHandle struct {
	payload []byte
	result  chan error
}

type Peer struct {
	ID        uint64
	worker    *Worker
	conn      net.Conn
	sendQueue chan PSendHandle
	kill      chan *sync.WaitGroup
	killOnce  uint32
}

func NewPeer(worker *Worker, conn net.Conn) *Peer {
	return &Peer{
		ID:        0,
		worker:    worker,
		conn:      conn,
		sendQueue: make(chan PSendHandle, 128),
		kill:      make(chan *sync.WaitGroup, 2),
		killOnce:  0,
	}
}

func (p *Peer) Init() {
	go p.messageSender()
	go p.messageReceiver()
}
func (p *Peer) messageSender() {
	for {
		var cmd PSendHandle
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
				cmd.result <- errors.Wrap(err, "failed to send messageReceiver to setupTestPeer")
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
func (p *Peer) messageReceiver() {
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
		c, _ := p.worker.recvQueue.LoadOrStore(opcode, WReceiveHandle{hub: make(chan Message), lock: make(chan struct{}, 1)})
		recv := c.(WReceiveHandle)
		select {
		case recv.hub <- msg:
			recv.lock <- struct{}{}
			<-recv.lock
		}
	}
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
func (p *Peer) SetWorker(worker *Worker) {
	p.worker = worker
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
func (p *Peer) DecodeMessage(buf []byte) (Opcode, Message, error) {
	reader := payload.NewReader(buf)
	opcode, err := reader.ReadByte()
	if err != nil {
		return OpcodeNil, nil, errors.Wrap(err, "failed to read opcode")
	}
	message, err := MessageFromOpcode(Opcode(opcode))
	if err != nil {
		return Opcode(opcode), nil, errors.Wrap(err, "opcode <-> messageReceiver pairing not registered")
	}
	message, err = message.Read(reader)
	if err != nil {
		return Opcode(opcode), nil, errors.Wrap(err, "failed to read messageReceiver contents")
	}
	return Opcode(opcode), message, nil
}

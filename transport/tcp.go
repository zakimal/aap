package transport

import (
	"github.com/pkg/errors"
	"net"
	"strconv"
	"time"
)

const TIMEOUT = 3 * time.Second

type tcp struct{}

func NewTCP() tcp {
	return tcp{}
}

func (t tcp) String() string {
	return "TCP"
}

func (t tcp) Listen(host string, port uint16) (net.Listener, error) {
	if net.ParseIP(host) == nil {
		return nil, errors.Errorf("wrong LocalIP: %s", host)
	}
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(port)))
	if err != nil {
		return nil, err
	}
	return listener, nil
}

func (t tcp) Dial(address string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", address, TIMEOUT)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (t tcp) IP(address net.Addr) net.IP {
	return address.(*net.TCPAddr).IP
}

func (t tcp) Port(address net.Addr) uint16 {
	return uint16(address.(*net.TCPAddr).Port)
}

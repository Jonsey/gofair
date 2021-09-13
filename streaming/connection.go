package streaming

import (
	"bufio"
	"crypto/tls"

	"github.com/belmegatron/gofair/streaming/models"
)

type TLSConnection struct {
	ID   int32
	conn *tls.Conn
}

func (conn *TLSConnection) Write(b []byte) (int, error) {
	return conn.conn.Write(b)
}

func (conn *TLSConnection) Stop() {
	conn.conn.Close()
}

func NewTLSConnection(destination string, certs *tls.Certificate) (*TLSConnection, error) {

	connection := new(TLSConnection)

	cfg := &tls.Config{Certificates: []tls.Certificate{*certs}}
	conn, err := tls.Dial("tcp", destination, cfg)

	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	buf, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	connectionMessage := new(models.ConnectionMessage)
	err = connectionMessage.UnmarshalJSON(buf)
	if err != nil {
		return nil, err
	}

	// Check we have a valid connection ID
	if connectionMessage.ConnectionID == "" {
		return nil, &ConnectionError{}
	}

	connection.conn = conn

	return connection, nil
}
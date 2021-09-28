package streaming

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"

	"github.com/sirupsen/logrus"

	"github.com/jonsey/gofair"
	"github.com/jonsey/gofair/streaming/models"
)

type IMarketStream interface {
	Subscribe(marketFilter *models.MarketFilter, marketDataFilter *models.MarketDataFilter)
	OnSubscribe(ChangeMessage models.MarketChangeMessage)
	OnResubscribe(ChangeMessage models.MarketChangeMessage)
	OnHeartbeat(ChangeMessage models.MarketChangeMessage)
	OnUpdate(ChangeMessage models.MarketChangeMessage)
}

type IOrderStream interface {
	OnSubscribe(ChangeMessage models.OrderChangeMessage)
	OnResubscribe(ChangeMessage models.OrderChangeMessage)
	OnHeartbeat(ChangeMessage models.OrderChangeMessage)
	OnUpdate(ChangeMessage models.OrderChangeMessage)
}

type Listener struct {
	uniqueID     int32
	connectionID string
	endpoint     string
	log          *logrus.Logger
	conn         *tls.Conn
	client       *gofair.Client
	scanner      bufio.Scanner

	MarketStream IMarketStream
	OrderStream  IOrderStream

	marketSubscriptionRequest chan models.MarketSubscriptionMessage
	orderSubscriptionRequest  chan models.OrderSubscriptionMessage
	killChannel               chan int
	ErrorChannel              chan error

	marketUpdates chan MarketBook
	// TODO: Change this to correct type
	orderUpdates chan interface{}
}

// NewListener creates a Listener struct
func NewListener(client *gofair.Client, endpoint string, log *logrus.Logger) (*Listener, error) {
	l := new(Listener)
	l.client = client
	l.endpoint = endpoint
	l.log = log

	if endpoint != gofair.Endpoints.Stream && endpoint != gofair.Endpoints.StreamIntegration {
		return nil, &EndpointError{}
	}

	l.marketSubscriptionRequest = make(chan models.MarketSubscriptionMessage, 64)
	l.killChannel = make(chan int)
	l.marketUpdates = make(chan MarketBook, 64)
	l.ErrorChannel = make(chan error)

	l.MarketStream = NewMarketStream(l, l.log, &l.marketUpdates)
	l.OrderStream = NewOrderStream(l, l.log)

	return l, nil
}

// Start performs the Connection and Authentication steps and initializes the read/write goroutines
func (l *Listener) Start(errChan *chan error) error {

	err := l.connect()

	if err != nil {
		return err
	}

	err = l.authenticate()

	if err != nil {
		return err
	}

	go l.readPump(errChan)
	go l.writePump(errChan)

	return nil
}

// Stop closes the connection and kills the associated read/write goroutines
func (l *Listener) Stop() error {

	l.killChannel <- 1

	err := l.conn.Close()

	if err != nil {
		return err
	}

	return nil
}

func (l *Listener) connect() error {

	cfg := &tls.Config{Certificates: []tls.Certificate{*l.client.Certificates}}
	conn, err := tls.Dial("tcp", l.endpoint, cfg)

	if err != nil {
		return err
	}

	c := bufio.NewReader(conn)
	buf, _, err := c.ReadLine()
	if err != nil {
		return err
	}

	connectionMessage := new(models.ConnectionMessage)
	err = connectionMessage.UnmarshalJSON(buf)
	if err != nil {
		return err
	}

	if connectionMessage.ConnectionID == "" {
		return &ConnectionError{}
	}

	l.connectionID = connectionMessage.ConnectionID
	l.conn = conn
	// This scanner allows us to keep reading bytes from the connection until we encounter "\r\n"
	l.scanner = *bufio.NewScanner(l.conn)
	l.scanner.Split(ScanCRLF)

	return nil
}

func (l *Listener) write(b []byte) (int, error) {
	// Every message is in json & terminated with a line feed (CRLF)
	b = append(b, []byte{'\r', '\n'}...)
	return l.conn.Write(b)
}

func (l *Listener) read() ([]byte, error) {

	l.scanner.Scan()

	if err := l.scanner.Err(); err != nil {
		return []byte{}, err
	}

	return l.scanner.Bytes(), nil
}

func (l *Listener) authenticate() error {

	if l.conn == nil {
		return &NoConnectionError{}
	}

	authenticationMessage := new(models.AuthenticationMessage)
	authenticationMessage.SetID(l.uniqueID)
	authenticationMessage.AppKey = l.client.Config.AppKey
	authenticationMessage.Session = l.client.Session.SessionToken

	b, err := authenticationMessage.MarshalJSON()
	if err != nil {
		return err
	}

	_, err = l.write(b)
	if err != nil {
		return err
	}

	buf, err := l.read()
	if err != nil {
		return err
	}

	statusMessage := new(models.StatusMessage)
	err = statusMessage.UnmarshalJSON(buf)
	if err != nil {
		return err
	}

	if statusMessage.StatusCode == "FAILURE" {

		l.log.WithFields(logrus.Fields{
			"errorCode":    statusMessage.ErrorCode,
			"errorMessage": statusMessage.ErrorMessage,
		}).Error("Failed to Authenticate")

		return &AuthenticationError{}
	}

	l.log.Debug("Authenticated")

	return nil
}

// dropCR drops a terminal \r from the data.
func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

func ScanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(data, []byte{'\r', '\n'}); i >= 0 {
		// We have a full newline-terminated line.
		return i + 2, dropCR(data[0:i]), nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), dropCR(data), nil
	}
	// Request more data.
	return 0, nil, nil
}

func (l *Listener) readPump(errChan *chan error) {

	if l.conn == nil {
		err := new(NoConnectionError)
		*errChan <- err
		return
	}

	for {
		select {
		case <-l.killChannel:
			return
		default:
			buf, err := l.read()
			if err != nil {
				*errChan <- err
				return
			}

			// Unmarshal raw bytes to JSON
			tmp := make(map[string]json.RawMessage)
			var op string
			err = json.Unmarshal(buf, &tmp)
			if err != nil {
				*errChan <- err
				return
			}

			// Peek to see the op code
			err = json.Unmarshal(tmp["op"], &op)
			if err != nil {
				*errChan <- err
				return
			}

			l.onData(op, buf)
		}
	}
}

func (l *Listener) writePump(errChan *chan error) {
	for {
		select {

		case <-l.killChannel:
			return

		case marketSubscriptionMessage := <-l.marketSubscriptionRequest:
			b, err := marketSubscriptionMessage.MarshalJSON()
			if err != nil {
				*errChan <- err
				return
			}

			l.write(b)

		case orderSubscriptionMessage := <-l.orderSubscriptionRequest:
			b, err := orderSubscriptionMessage.MarshalJSON()
			if err != nil {
				*errChan <- err
				return
			}
			l.write(b)

		}
	}
}

func (l *Listener) onData(op string, data []byte) {

	switch op {
	case "connection":
		l.onConnection(data)
	case "status":
		l.onStatus(data)
	case "mcm":
		l.onMarketChangeMessage(l.MarketStream, data)
	case "ocm":
		l.onOrderChangeMessage(l.OrderStream, data)
	}
}

func (l *Listener) onConnection(data []byte) {
	l.log.Debug("Connected")
}

func (l *Listener) onStatus(data []byte) {
	l.log.Debug("Status Message Received")
}

func (l *Listener) onMarketChangeMessage(Stream IMarketStream, data []byte) {

	marketChangeMessage := new(models.MarketChangeMessage)

	err := marketChangeMessage.UnmarshalJSON(data)
	if err != nil {
		l.log.Error("Failed to unmarshal MarketChangeMessage.")
		return
	}

	switch marketChangeMessage.Ct {
	case "SUB_IMAGE":
		Stream.OnSubscribe(*marketChangeMessage)
	case "RESUB_DELTA":
		Stream.OnResubscribe(*marketChangeMessage)
	case "HEARTBEAT":
		Stream.OnHeartbeat(*marketChangeMessage)
	default:
		Stream.OnUpdate(*marketChangeMessage)
	}
}

func (l *Listener) onOrderChangeMessage(Stream IOrderStream, data []byte) {

	orderChangeMessage := new(models.OrderChangeMessage)

	err := orderChangeMessage.UnmarshalJSON(data)
	if err != nil {
		l.log.Error("Failed to unmarshal OrderChangeMessage.")
		return
	}

	switch orderChangeMessage.Ct {
	case "SUB_IMAGE":
		Stream.OnSubscribe(*orderChangeMessage)
	case "RESUB_DELTA":
		Stream.OnResubscribe(*orderChangeMessage)
	case "HEARTBEAT":
		Stream.OnHeartbeat(*orderChangeMessage)
	default:
		Stream.OnUpdate(*orderChangeMessage)
	}
}

package streaming

import (
	"crypto/tls"

	"github.com/belmegatron/gofair/streaming/models"
)

type StreamChannels struct {
	// Outgoing Requests
	marketSubscriptionRequest chan models.MarketSubscriptionMessage
	orderSubscriptionRequest  chan models.OrderSubscriptionMessage

	// Incoming Responses
	MarketUpdate chan MarketBook
	// TODO: Fix this
	OrderUpdate                chan interface{}
	MarketSubscriptionResponse chan MarketSubscriptionResponse
	Err                        chan error
}

func newStreamChannels() *StreamChannels {

	channels := new(StreamChannels)

	// Set up Outgoing Request Channels
	channels.marketSubscriptionRequest = make(chan models.MarketSubscriptionMessage, 64)
	channels.orderSubscriptionRequest = make(chan models.OrderSubscriptionMessage, 1)

	// Set up Incoming Response Channels
	channels.MarketUpdate = make(chan MarketBook, 64)
	channels.MarketSubscriptionResponse = make(chan MarketSubscriptionResponse, 64)
	channels.Err = make(chan error)

	return channels
}

type Stream struct {
	requestUID   int32
	endpoint     string
	certs        *tls.Certificate
	appKey       string
	sessionToken string

	Channels *StreamChannels
	session  *Session
}

// NewStreamClient blah blah
func NewStream(endpoint string, certs *tls.Certificate, appKey string, sessionToken string) (*Stream, error) {

	if endpoint != LiveEndpoint && endpoint != IntegrationEndpoint {
		return nil, &EndpointError{}
	}

	stream := new(Stream)
	stream.endpoint = endpoint
	stream.certs = certs
	stream.appKey = appKey
	stream.sessionToken = sessionToken
	stream.Channels = newStreamChannels()

	return stream, nil
}

// Start performs the Connection and Authentication steps and initializes the read/write goroutines
func (stream *Stream) Start() error {

	session, err := NewSession(stream.endpoint, stream.certs, stream.appKey, stream.sessionToken)
	if err != nil {
		return err
	}

	stream.session = session

	return nil
}

// Stop tears down the underlying TLS session to the Streaming endpoint
func (stream *Stream) Stop() {
	stream.session.Stop()
}

func (stream *Stream) SubscribeToMarkets(marketFilter *models.MarketFilter, marketDataFilter *models.MarketDataFilter) {

	request := models.MarketSubscriptionMessage{MarketFilter: marketFilter, MarketDataFilter: marketDataFilter}
	request.SetID(stream.requestUID)
	stream.Channels.marketSubscriptionRequest <- request
}

func (stream *Stream) SubscribeToOrders() {
	request := models.OrderSubscriptionMessage{SegmentationEnabled: true}
	request.SetID(stream.requestUID)
	stream.Channels.orderSubscriptionRequest <- request
}

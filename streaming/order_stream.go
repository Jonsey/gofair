package streaming

import (
	"github.com/jonsey/gofair/streaming/models"
	"github.com/sirupsen/logrus"
)

type OrderStream struct {
	cache      OrderCache
	listener   *Listener
	log        *logrus.Logger
	InitialClk string
	Clk        string
}

func NewOrderStream(listener *Listener, log *logrus.Logger) *OrderStream {
	orderStream := new(OrderStream)
	orderStream.cache = make(OrderCache)
	orderStream.listener = listener
	orderStream.log = log
	return orderStream
}

func (orderStream *OrderStream) Subscribe(orderSubscription models.OrderSubscriptionMessage) {
	orderStream.listener.orderSubscriptionRequest <- orderSubscription
}

func (orderStream *OrderStream) OnSubscribe(orderChangeMessage models.OrderChangeMessage) {

}

func (orderStream *OrderStream) OnResubscribe(orderChangeMessage models.OrderChangeMessage) {

}

func (orderStream *OrderStream) OnHeartbeat(orderChangeMessage models.OrderChangeMessage) {

}

func (orderStream *OrderStream) OnUpdate(orderChangeMessage models.OrderChangeMessage) {

	if orderStream.InitialClk == "" {
		orderStream.InitialClk = orderChangeMessage.Clk
	}

	orderStream.Clk = orderChangeMessage.Clk

	for _, orderMarketChange := range orderChangeMessage.Oc {
		
		// Check if a cache for the given Market ID exists
		_, found := orderStream.cache[orderMarketChange.ID]
		if !found {
			orderStream.cache[orderMarketChange.ID] = MarketOrderCache{MarketID: orderMarketChange.ID}
		}

		// Check if a cache for the given MarketID/RunnerID combination exists
		for _, orderRunnerChange := range orderMarketChange.Orc {

			_, found = orderStream.cache[orderMarketChange.ID].RunnerOrders[int(orderRunnerChange.ID)]
			if !found {
				orderStream.cache[orderMarketChange.ID].RunnerOrders[int(orderRunnerChange.ID)] = RunnerOrderCache{}
			}
			
			// TODO: Update Cache

		}
	}
}

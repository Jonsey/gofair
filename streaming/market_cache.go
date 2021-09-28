package streaming

import (
	"sort"

	"github.com/jonsey/gofair/streaming/models"
)

func CreateMarketCache(changeMessage *models.MarketChangeMessage, marketChange *models.MarketChange) *MarketCache {
	cache := &MarketCache{
		&changeMessage.Pt,
		marketChange.ID,
		&marketChange.Tv,
		marketChange.MarketDefinition,
		make(map[int64]RunnerCache),
	}
	for _, runnerChange := range marketChange.Rc {
		cache.Runners[runnerChange.ID] = *CreateRunnerCache(runnerChange)
	}
	return cache
}

func CreateRunnerCache(change *models.RunnerChange) *RunnerCache {

	// create traded data structure
	var traded Available
	for _, i := range change.Trd {
		traded.Prices = append(
			traded.Prices,
			PriceSize{i[0], i[1]},
		)
	}
	traded.Reverse = false

	// create availableToBack data structure
	var availableToBack Available
	for _, i := range change.Atb {
		availableToBack.Prices = append(
			availableToBack.Prices,
			PriceSize{i[0], i[1]},
		)
	}
	availableToBack.Reverse = true

	// create availableToLay data structure
	var availableToLay Available
	for _, i := range change.Atl {
		availableToLay.Prices = append(
			availableToLay.Prices,
			PriceSize{i[0], i[1]},
		)
	}
	availableToLay.Reverse = false

	// create startingPriceBack data structure
	var startingPriceBack Available
	for _, i := range change.Spb {
		startingPriceBack.Prices = append(
			startingPriceBack.Prices,
			PriceSize{i[0], i[1]},
		)
	}
	startingPriceBack.Reverse = false

	// create startingPriceLay data structure
	var startingPriceLay Available
	for _, i := range change.Spl {
		startingPriceLay.Prices = append(
			startingPriceLay.Prices,
			PriceSize{i[0], i[1]},
		)
	}
	startingPriceLay.Reverse = false

	// create bestAvailableToBack data structure
	var bestAvailableToBack AvailablePosition
	for _, i := range change.Batb {
		bestAvailableToBack.Prices = append(
			bestAvailableToBack.Prices,
			PositionPriceSize{i[0], i[1], i[2]},
		)
	}
	bestAvailableToBack.Reverse = false

	// create bestAvailableToLay data structure
	var bestAvailableToLay AvailablePosition
	for _, i := range change.Batl {
		bestAvailableToLay.Prices = append(
			bestAvailableToLay.Prices,
			PositionPriceSize{i[0], i[1], i[2]},
		)
	}
	bestAvailableToLay.Reverse = false

	// create bestDisplayAvailableToBack data structure
	var bestDisplayAvailableToBack AvailablePosition
	for _, i := range change.Bdatb {
		bestDisplayAvailableToBack.Prices = append(
			bestDisplayAvailableToBack.Prices,
			PositionPriceSize{i[0], i[1], i[2]},
		)
	}
	bestDisplayAvailableToBack.Reverse = false

	// create bestDisplayAvailableToLay data structure
	var bestDisplayAvailableToLay AvailablePosition
	for _, i := range change.Bdatl {
		bestDisplayAvailableToLay.Prices = append(
			bestDisplayAvailableToLay.Prices,
			PositionPriceSize{i[0], i[1], i[2]},
		)
	}
	bestDisplayAvailableToLay.Reverse = false

	cache := &RunnerCache{
		SelectionId:                change.ID,
		LastTradedPrice:            &change.Ltp,
		TradedVolume:               &change.Tv,
		StartingPriceNear:          &change.Spn,
		StartingPriceFar:           &change.Spf,
		Traded:                     &traded,
		AvailableToBack:            &availableToBack,
		AvailableToLay:             &availableToLay,
		StartingPriceBack:          &startingPriceBack,
		StartingPriceLay:           &startingPriceLay,
		BestAvailableToBack:        &bestAvailableToBack,
		BestAvailableToLay:         &bestAvailableToLay,
		BestDisplayAvailableToBack: &bestDisplayAvailableToBack,
		BestDisplayAvailableToLay:  &bestDisplayAvailableToLay,
	}
	return cache
}

type AvailableInterface interface {
	Clear()
	Sort()
	UpdatePrice(int, []float64)
	AppendPrice([]float64)
	RemovePrice(int)
	Update([][]float64)
}

type PriceSize struct {
	Price float64
	Size  float64
}

// sort.Interface for []PriceSize based on price
type ByPrice []PriceSize

func (a ByPrice) Len() int           { return len(a) }
func (a ByPrice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByPrice) Less(i, j int) bool { return a[i].Price < a[j].Price }

func (a ByPrice) GetLastItem() PriceSize {

	if len(a) == 1 {
		return a[0]
	} else if len(a) > 1 {
		return a[len(a)-1]
	}

	return PriceSize{}
}

type PositionPriceSize struct {
	Position float64
	Price    float64
	Size     float64
}

// sort.Interface for []PositionPriceSize based on position
type ByPosition []PositionPriceSize

func (a ByPosition) Len() int           { return len(a) }
func (a ByPosition) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByPosition) Less(i, j int) bool { return a[i].Position < a[j].Position }

func (a ByPosition) GetLastItem() PositionPriceSize {

	if len(a) == 1 {
		return a[0]
	} else if len(a) > 1 {
		return a[len(a)-1]
	}

	return PositionPriceSize{}
}

type AvailablePosition struct {
	Prices  ByPosition
	Reverse bool
}

func (available *AvailablePosition) Clear() {
	available.Prices = nil
}

func (available *AvailablePosition) Sort() {
	if available.Reverse {
		sort.Sort(sort.Reverse(ByPosition(available.Prices)))
	} else {
		sort.Sort(ByPosition(available.Prices))
	}
}

func (available *AvailablePosition) UpdatePrice(count int, update []float64) {
	available.Prices[count] = PositionPriceSize{update[0], update[1], update[2]}
}

func (available *AvailablePosition) AppendPrice(update []float64) {
	available.Prices = append(available.Prices, PositionPriceSize{update[0], update[1], update[2]})
}

func (available *AvailablePosition) RemovePrice(i int) {
	s := available.Prices
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	available.Prices = s[:len(s)-1]
}

func (available *AvailablePosition) Update(updates [][]float64) {
	for _, update := range updates {
		updated := false
		for count, trade := range available.Prices {
			if trade.Price == update[0] {
				if update[2] == 0 {
					available.RemovePrice(count)
					updated = true
					break
				} else {
					available.UpdatePrice(count, update)
					updated = true
					break
				}
			}
		}
		if !updated && update[2] != 0 {
			available.AppendPrice(update)
		}
	}
	available.Sort()
}

type Available struct {
	Prices  ByPrice
	Reverse bool
}

func (available *Available) Clear() {
	available.Prices = nil
}

func (available *Available) Sort() {
	if available.Reverse {
		sort.Sort(sort.Reverse(ByPrice(available.Prices)))
	} else {
		sort.Sort(ByPrice(available.Prices))
	}
}

func (available *Available) UpdatePrice(count int, update []float64) {
	available.Prices[count] = PriceSize{update[0], update[1]}
}

func (available *Available) AppendPrice(update []float64) {
	available.Prices = append(available.Prices, PriceSize{update[0], update[1]})
}

func (available *Available) RemovePrice(i int) {
	s := available.Prices
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	available.Prices = s[:len(s)-1]
}

func (available *Available) Update(updates [][]float64) {
	for _, update := range updates {
		updated := false
		for count, trade := range available.Prices {
			if trade.Price == update[0] {
				if update[1] == 0 {
					available.RemovePrice(count)
					updated = true
					break
				} else {
					available.UpdatePrice(count, update)
					updated = true
					break
				}
			}
		}
		if !updated && update[1] != 0 {
			available.AppendPrice(update)
		}
	}
	available.Sort()
}

type RunnerCache struct {
	SelectionId                int64
	LastTradedPrice            *float64
	TradedVolume               *float64
	StartingPriceNear          *float64
	StartingPriceFar           *float64
	Traded                     *Available
	AvailableToBack            *Available
	AvailableToLay             *Available
	StartingPriceBack          *Available
	StartingPriceLay           *Available
	BestAvailableToBack        *AvailablePosition
	BestAvailableToLay         *AvailablePosition
	BestDisplayAvailableToBack *AvailablePosition
	BestDisplayAvailableToLay  *AvailablePosition
}

func (cache *RunnerCache) UpdateCache(change *models.RunnerChange) {
	if change.Ltp != 0 {
		*cache.LastTradedPrice = change.Ltp
	}
	if change.Tv != 0 {
		*cache.TradedVolume = change.Tv
	}
	if change.Spn != 0 {
		*cache.StartingPriceNear = change.Spn
	}
	if change.Spf != 0 {
		*cache.StartingPriceFar = change.Spf
	}
	if len(change.Trd) > 0 {
		cache.Traded.Update(change.Trd)
	}
	if len(change.Atb) > 0 {
		cache.AvailableToBack.Update(change.Atb)
	}
	if len(change.Atl) > 0 {
		cache.AvailableToLay.Update(change.Atl)
	}
	if len(change.Spb) > 0 {
		cache.StartingPriceBack.Update(change.Spb)
	}
	if len(change.Spl) > 0 {
		cache.StartingPriceLay.Update(change.Spl)
	}
	if len(change.Batb) > 0 {
		cache.BestAvailableToBack.Update(change.Batb)
	}
	if len(change.Batl) > 0 {
		cache.BestAvailableToLay.Update(change.Batl)
	}
	if len(change.Bdatb) > 0 {
		cache.BestDisplayAvailableToBack.Update(change.Bdatb)
	}
	if len(change.Bdatl) > 0 {
		cache.BestDisplayAvailableToLay.Update(change.Bdatl)
	}
}

type MarketCache struct {
	PublishTime      *int64
	MarketID         string
	TradedVolume     *float64
	MarketDefinition *models.MarketDefinition
	Runners          map[int64]RunnerCache
}

func (cache *MarketCache) UpdateCache(changeMessage *models.MarketChangeMessage, marketChange *models.MarketChange) {
	*cache.PublishTime = changeMessage.Pt

	if marketChange.MarketDefinition != nil {
		*cache.MarketDefinition = *marketChange.MarketDefinition
	}
	if marketChange.Tv != 0 {
		*cache.TradedVolume = marketChange.Tv
	}
	if marketChange.Rc != nil {
		for _, runnerChange := range marketChange.Rc {
			if runnerCache, ok := cache.Runners[runnerChange.ID]; ok {
				runnerCache.UpdateCache(runnerChange)
			} else {
				cache.Runners[runnerChange.ID] = *CreateRunnerCache(runnerChange)
			}
		}
	}
}

func (cache *MarketCache) GetRunnerDefinition(selectionId int64) models.RunnerDefinition {
	for i := range cache.MarketDefinition.Runners {
		if cache.MarketDefinition.Runners[i].ID == selectionId {
			return *cache.MarketDefinition.Runners[i]
		}
	}
	return models.RunnerDefinition{}
}

// snap functions

func (cache *RunnerCache) Snap(definition models.RunnerDefinition) Runner {

	exchangePrices := ExchangePrices{
		BestAvailableToBack: cache.BestAvailableToBack.Prices.GetLastItem(),
		BestAvailableToLay:  cache.BestAvailableToLay.Prices.GetLastItem(),
		AvailableToBack:     cache.AvailableToBack.Prices.GetLastItem(),
		AvailableToLay:      cache.AvailableToLay.Prices.GetLastItem(),
		TradedVolume:        cache.Traded.Prices.GetLastItem(),
	}
	return Runner{
		SelectionID:      cache.SelectionId,
		Handicap:         definition.Hc,
		Status:           definition.Status,
		AdjustmentFactor: definition.AdjustmentFactor,
		LastPriceTraded:  *cache.LastTradedPrice,
		TotalMatched:     *cache.TradedVolume,
		RemovalDate:      definition.RemovalDate,
		EX:               exchangePrices,
	}
}

func (cache *MarketCache) Snap() MarketBook {
	runners := []Runner{}

	for _, runner := range cache.Runners {
		runnerDefinition := cache.GetRunnerDefinition(runner.SelectionId)
		runners = append(runners, runner.Snap(runnerDefinition))
	}
	return MarketBook{
		PublishTime:           *cache.PublishTime,
		MarketID:              cache.MarketID,
		Status:                cache.MarketDefinition.Status,
		BetDelay:              cache.MarketDefinition.BetDelay,
		BspReconciled:         cache.MarketDefinition.BspReconciled,
		Complete:              cache.MarketDefinition.Complete,
		InPlay:                cache.MarketDefinition.InPlay,
		NumberOfWinners:       cache.MarketDefinition.NumberOfWinners,
		NumberOfRunners:       len(cache.Runners),
		NumberOfActiveRunners: cache.MarketDefinition.NumberOfActiveRunners,
		TotalMatched:          *cache.TradedVolume,
		CrossMatching:         cache.MarketDefinition.CrossMatching,
		RunnersVoidable:       cache.MarketDefinition.RunnersVoidable,
		Version:               cache.MarketDefinition.Version,
		Runners:               runners,
	}
}

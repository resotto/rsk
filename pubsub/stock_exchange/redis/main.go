package main

import (
	"container/list"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"math/rand"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

const (
	orderChannel        = "order_channel"
	currentPriceChannel = "current_price_channel"

	hundredPercent    = 10_000
	marketProbability = 2000
	orderProbability  = 3000
	alertProbability  = 100

	maxPrice         = 10_000
	initialPrice     = maxPrice / 2
	priceRange       = 20
	halfPriceRange   = priceRange / 2
	orderHistorySize = 5
)

var (
	cluster                                    bool
	instances, semaphore                       int
	participants, funds, volatility, durations int

	cli     redis.UniversalClient
	traders []*Trader
)

func parseFlags() {
	flag.BoolVar(&cluster, "cluster", false, "")
	flag.IntVar(&instances, "instances", -1, "")
	flag.IntVar(&semaphore, "semaphore", 20_000, "")
	flag.IntVar(&participants, "participants", 1_000_000, "")
	flag.IntVar(&funds, "funds", 1000000, "")
	flag.IntVar(&volatility, "volatility", 0, "")
	flag.IntVar(&durations, "durations", 20, "")

	flag.Parse()
}

func main() {
	parseFlags()

	if semaphore <= maxPrice {
		log.Fatal("semaphore is too small to initialize the order book with max price =", maxPrice)
	}

	rand.New(rand.NewSource(time.Now().UnixMicro()))

	if cluster {
		addrs := make([]string, 0)
		for i := 0; i < instances; i++ {
			addrs = append(addrs, ":"+strconv.Itoa(7000+i))
		}
		cli = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: addrs,
		})
	} else {
		cli = redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durations)*time.Second)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(semaphore)

	orderBook := newOrderBook()
	traders = make([]*Trader, maxPrice+participants)

	eg.Go(func() error { return processOrders(ctx, orderBook) })

	for i := range maxPrice + participants {
		traders[i] = newTrader(i)
		if i < maxPrice {
			pubilshInitialOrder(ctx, i)
		} else {
			eg.Go(func() error { return updateTradersCurrentPrice(ctx, i) })
			eg.Go(func() error { return trade(ctx, i) })
		}
	}

	err := eg.Wait()
	if err != context.DeadlineExceeded {
		fmt.Println("result (error):", err.Error())
	}
	if cluster {
		fmt.Println("cluster:", cluster)
		fmt.Println("instances:", instances)
	}
	fmt.Println("semaphore:", semaphore)
	fmt.Println("participants:", participants)
	fmt.Println("volatility:", volatility)
	fmt.Println("durations:", durations)
}

func processOrders(ctx context.Context, orderBook *OrderBook) error {
	ch := cli.SSubscribe(ctx, orderChannel).Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message := <-ch:
			if order := getResource[Order](message); order != nil {
				orderBook.process(ctx, order)
			}
		}
	}
}

func updateTradersCurrentPrice(ctx context.Context, i int) error {
	ch := cli.SSubscribe(ctx, currentPriceChannel).Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message := <-ch:
			if currentPrice := getResource[CurrentPrice](message); currentPrice != nil {
				traders[i].updateCurrentPrice(currentPrice)
			}
		}
	}
}

func trade(ctx context.Context, i int) error {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("closing", strconv.Itoa(i), "th trader's publisher")
			return ctx.Err()
		default:
			if rand.Intn(hundredPercent) < orderProbability+volatility {
				if order := traders[i].prepareOrder(); order != nil {
					publish(ctx, orderChannel, order)
				}
			}
			if rand.Intn(hundredPercent) < alertProbability {
				traders[i].setAlertPrice()
			}
			time.Sleep(1000 * time.Millisecond)
		}
	}
}

func pubilshInitialOrder(ctx context.Context, i int) {
	var side OrderSide
	price := initialPrice
	if i < initialPrice {
		side = Ask
		price += i + 1
	} else {
		side = Bid
		price -= i - initialPrice
	}
	publish(ctx, orderChannel, traders[i].newOrder(uuid.NewString(), Create, Limit, side, &price, funds/price))
}

func getResource[T resource](message *redis.Message) *T {
	if message.Payload == "ping" {
		return nil
	}
	resource, err := deserialize[T]([]byte(message.Payload))
	if err != nil {
		fmt.Println(resource.string(), "deserialization failed", err.Error())
		return nil
	}
	return &resource
}

func publish(ctx context.Context, channel string, a resource) {
	message, err := json.Marshal(a)
	if err != nil {
		fmt.Println(a.string(), "serialization failed", err.Error())
	} else if _, err := cli.SPublish(ctx, channel, message).Result(); err != nil && err != context.DeadlineExceeded {
		fmt.Println("failed to publish", a.string(), err.Error())
	}
}

func deserialize[T any](bytes []byte) (T, error) {
	var result T
	err := json.Unmarshal(bytes, &result)
	return result, err
}

type resource interface {
	string() string
}

type Trader struct {
	TraderID, Funds, Shares, openOrderCount int
	RecentOrders                            *list.List
	AlertPrice                              *int
	CurrentPrice, Alert                     *CurrentPrice
	Mu                                      sync.Mutex
}

func (t *Trader) executeAsk(price, quantity int) {
	t.Shares -= quantity
	t.Funds += price * quantity
}

func (t *Trader) executeBid(price, quantity int) {
	t.Funds -= price * quantity
	t.Shares += quantity
}

func (t *Trader) getOffsetPrice(forAsk bool) *int {
	targetPrice := t.CurrentPrice.Price - rand.Intn(halfPriceRange)
	if forAsk {
		targetPrice = t.CurrentPrice.Price + rand.Intn(halfPriceRange)
	}
	return &targetPrice
}

func (t *Trader) prepareOrder() *Order {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	orderType := newOrderType()
	if 0 < t.openOrderCount {
		cur := t.hasModifiableOrder()
		if cur == nil {
			return nil
		}
		orderEvent := OrderEvent(1 + rand.Intn(2))
		if orderEvent == Cancel {
			orderType = cur.Type
		}
		return t.newOrder(cur.OrderID, orderEvent, orderType, cur.Side, cur.Price, cur.RemainingQuantity)
	} else if 0 < t.Shares {
		targetPrice := t.getOffsetPrice(true)
		if orderType == Market {
			targetPrice = nil
		}
		return t.newOrder(uuid.NewString(), Create, orderType, Ask, targetPrice, 1+rand.Intn(t.Shares))
	}
	var targetPrice, expectedPrice *int = nil, &t.CurrentPrice.Price
	if orderType == Limit {
		targetPrice = t.getOffsetPrice(false)
		expectedPrice = targetPrice
	}
	return t.newOrder(uuid.NewString(), Create, orderType, Bid, targetPrice, t.Funds / *expectedPrice)
}

func (t *Trader) hasModifiableOrder() *Order {
	e := t.RecentOrders.Front()
	if e == nil {
		return nil
	}
	cur := e.Value.(*Order)
	if cur.isSettled() {
		return nil
	}
	return cur
}

func (t *Trader) newOrder(orderID string, event OrderEvent, orderType OrderType, side OrderSide, price *int, quantity int) *Order {
	if event != Cancel {
		t.openOrderCount++
	}
	return &Order{
		OrderID:           orderID,
		TraderID:          t.TraderID,
		Event:             event,
		Type:              orderType,
		Side:              side,
		Price:             price,
		Quantity:          quantity,
		RemainingQuantity: quantity,
		Timestamp:         time.Now(),
	}
}

func (t *Trader) fillOrder(order *Order, price, quantity int) {
	switch order.Side {
	case Ask:
		t.executeAsk(price, quantity)
	case Bid:
		t.executeBid(price, quantity)
	}
	order.applyPrice(price)
	order.fill(quantity)
	if order.RemainingQuantity == 0 {
		order.applyEvent(Fill)
	} else {
		order.applyEvent(PartialFill)
	}
	t.updateHistory(order)
	t.openOrderCount--
}

func (t *Trader) updateHistory(order *Order) {
	t.RecentOrders.PushFront(order)
	if orderHistorySize < t.RecentOrders.Len() {
		t.RecentOrders.Remove(t.RecentOrders.Back())
	}
}

func (t *Trader) setAlertPrice() {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	t.Alert = nil
	targetPrice := t.getOffsetPrice(0 < t.Shares)
	t.AlertPrice = targetPrice
}

func (t *Trader) updateCurrentPrice(cp *CurrentPrice) {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	t.CurrentPrice = cp
	if t.AlertPrice != nil && *(t.AlertPrice) == cp.Price {
		t.Alert = cp
		t.AlertPrice = nil
	}
}

func (t *Trader) print() {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	fmt.Println("Funds:", t.Funds, "Shares:", t.Shares)
	fmt.Println()
	fmt.Print("Price Alert: ")
	if t.Alert != nil {
		fmt.Print(t.Alert.Price, " reached at ", t.Alert.Timestamp.Format(time.DateTime))
	}
	fmt.Print("\n")
	fmt.Println()
	fmt.Println("Recent orders:")
	for e := t.RecentOrders.Front(); e != nil; e = e.Next() {
		if o, ok := e.Value.(*Order); ok {
			if o.Type == Market {
				fmt.Println("-", o.Timestamp.Format(time.DateTime), o.Event.String(), o.Type.String(), o.Side.String(), "order with quantity =", o.RemainingQuantity)
			} else {
				fmt.Println("-", o.Timestamp.Format(time.DateTime), o.Event.String(), o.Type.String(), o.Side.String(), "order with price =", *o.Price, "with quantity =", o.RemainingQuantity)
			}
		} else {
			fmt.Println("type assertion to Order failed")
		}
	}
}

func newTrader(i int) *Trader {
	return &Trader{
		TraderID:     i,
		Funds:        funds,
		Shares:       []int{0, 1000}[rand.Intn(2)],
		RecentOrders: list.New(),
		CurrentPrice: newCurrentPrice(initialPrice),
	}
}

type OrderBook struct {
	Price, High, Low, Open, Close, Volume, Value int
	Orders                                       [][]list.List
	PriceSizes                                   [][]int
	OrderIDElmMap                                map[string]*list.Element
}

func (b OrderBook) string() string { return "order book" }

func newOrderBook() *OrderBook {
	return &OrderBook{
		Price:         initialPrice,
		High:          initialPrice,
		Low:           initialPrice,
		Open:          initialPrice,
		Close:         initialPrice,
		Orders:        [][]list.List{make([]list.List, maxPrice+1), make([]list.List, maxPrice+1)},
		PriceSizes:    [][]int{make([]int, maxPrice+1), make([]int, maxPrice+1)},
		OrderIDElmMap: make(map[string]*list.Element),
	}
}

func (b *OrderBook) process(ctx context.Context, order *Order) {
	switch order.Event {
	case Cancel:
		b.cancelOrder(order.OrderID)
	case Update:
		b.cancelOrder(order.OrderID)
		order.applyEvent(Create)
		fallthrough
	case Create:
		switch order.Type {
		case Limit:
			b.matchLimitOrder(ctx, order)
		case Market:
			b.matchMarketOrder(ctx, order)
		}
	}
}

func (b *OrderBook) matchLimitOrder(ctx context.Context, order *Order) {
	b.addOrder(order)
	b.matchOrder(*order.Price)
	if PartialFill <= order.Event {
		b.updatePrice(*order.Price)
		publish(ctx, currentPriceChannel, newCurrentPrice(b.Price))
		b.print()
	}
}

func (b *OrderBook) matchMarketOrder(ctx context.Context, order *Order) {
	b.addOrder(order)
	for {
		for !order.isSettled() && 0 < b.Orders[order.Side.opposite()][b.Price].Len() {
			b.matchOrder(b.Price)
		}
		b.print()
		if order.isSettled() {
			break
		}
		b.completeOrder(order.Side, b.Price, order.RemainingQuantity, true)
		b.updatePrice(b.getNextMarketPrice(order.Side))
		b.addOrder(order)
		publish(ctx, currentPriceChannel, newCurrentPrice(b.Price))
		b.print()
	}
}

func (b *OrderBook) matchOrder(price int) {
	askElm, bidElm := b.Orders[Ask][price].Front(), b.Orders[Bid][price].Front()
	if askElm != nil && bidElm != nil {
		askOrder, bidOrder := askElm.Value.(*Order), bidElm.Value.(*Order)
		askTrader, bidTrader := traders[askOrder.TraderID], traders[bidOrder.TraderID]
		askTrader.Mu.Lock()
		defer askTrader.Mu.Unlock()
		bidTrader.Mu.Lock()
		defer bidTrader.Mu.Unlock()
		askTraderSharesShort, bidTraderFundsShort := askTrader.Shares < askOrder.RemainingQuantity, bidTrader.Funds/price < bidOrder.RemainingQuantity
		if askTraderSharesShort {
			b.cancelOrder(askOrder.OrderID)
		}
		if bidTraderFundsShort {
			b.cancelOrder(bidOrder.OrderID)
		}
		if askTraderSharesShort || bidTraderFundsShort {
			return
		}
		minQuantity := min(askOrder.RemainingQuantity, bidOrder.RemainingQuantity)
		askTrader.fillOrder(askOrder, price, minQuantity)
		bidTrader.fillOrder(bidOrder, price, minQuantity)
		b.completeOrder(askOrder.Side, price, minQuantity, askOrder.Event == Fill)
		b.completeOrder(bidOrder.Side, price, minQuantity, bidOrder.Event == Fill)
		b.Volume += minQuantity
		b.Value += price * minQuantity
	}
}

func (b *OrderBook) getNextMarketPrice(side OrderSide) int {
	if side == Ask {
		return b.Price - 1
	}
	return b.Price + 1
}

func (b *OrderBook) updatePrice(price int) {
	b.Price = price
	if b.High < b.Price {
		b.High = b.Price
	}
	b.Low = min(b.Low, b.Price)
	b.Close = b.Price
}

func (b *OrderBook) cancelOrder(orderID string) {
	elm := b.OrderIDElmMap[orderID]
	delete(b.OrderIDElmMap, orderID)
	if elm != nil {
		order := elm.Value.(*Order)
		order.applyEvent(Cancel)
		b.PriceSizes[order.Side][*order.Price] -= order.RemainingQuantity
		b.Orders[order.Side][*order.Price].Remove(elm)
	}
}

func (b *OrderBook) addOrder(order *Order) {
	price := b.Price
	if order.Type == Market {
		order.applyPrice(price)
		b.OrderIDElmMap[order.OrderID] = b.Orders[order.Side][price].PushFront(order)
	} else {
		price = *order.Price
		b.OrderIDElmMap[order.OrderID] = b.Orders[order.Side][price].PushBack(order)
	}
	b.PriceSizes[order.Side][price] += order.RemainingQuantity
}

func (b *OrderBook) completeOrder(side OrderSide, price, quantity int, remove bool) {
	b.PriceSizes[side][price] -= quantity
	if remove {
		b.Orders[side][price].Remove(b.Orders[side][price].Front())
	}
}

func (b *OrderBook) print() {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()

	fmt.Println("Current price:", b.Price)
	fmt.Println("High:", b.High, "Low:", b.Low, "Open:", b.Open, "Close", b.Close, "Volume:", b.Volume, "Value:", b.Value)
	fmt.Println()
	stringFormat := "| %8s | %6s | %8s |\n"
	fmt.Printf(stringFormat, "Ask size", "Price", "Bid size")
	bidSum, over, under := 0, b.Price+halfPriceRange+1, b.Price-halfPriceRange-1
	for i := 0; i <= under; i++ {
		bidSum += b.PriceSizes[Bid][i]
	}
	for i, askSum := maxPrice, 0; under <= i; i-- {
		ask, bid := b.PriceSizes[Ask][i], b.PriceSizes[Bid][i]
		askSum += ask
		if i == over {
			fmt.Printf(stringFormat, convert(askSum), "Over", convert(bid))
		} else if i == b.Price {
			fmt.Printf(stringFormat, convert(ask), "> "+convert(i), convert(bid))
		} else if under < i && i < over {
			fmt.Printf(stringFormat, convert(ask), convert(i), convert(bid))
		} else if under == i {
			fmt.Printf(stringFormat, convert(ask), "Under", convert(bidSum))
		}
	}
	fmt.Println()
	traders[0].print()
	fmt.Println()
}

func convert(n int) string {
	if n == 0 {
		return ""
	}
	return strconv.Itoa(n)
}

type CurrentPrice struct {
	Price     int       `json:"price"`
	Timestamp time.Time `json:"timestamp"`
}

func newCurrentPrice(price int) *CurrentPrice {
	return &CurrentPrice{Price: price, Timestamp: time.Now()}
}

func (c CurrentPrice) string() string { return "current price" }

func min(vals ...int) int {
	min := math.MaxInt
	for _, v := range vals {
		if v < min {
			min = v
		}
	}
	return min
}

type Order struct {
	OrderID           string     `json:"order_id"`
	TraderID          int        `json:"trader_id"`
	Event             OrderEvent `json:"event"`
	Type              OrderType  `json:"type"`
	Side              OrderSide  `json:"side"`
	Price             *int       `json:"price"`
	Quantity          int        `json:"quantity"`
	RemainingQuantity int        `json:"remaining_quantity"`
	Timestamp         time.Time  `json:"timestamp"`
}

func (o Order) string() string { return "order" }

func (o *Order) applyPrice(price int) {
	o.Price = &price
}

func (o *Order) applyEvent(event OrderEvent) {
	o.Event = event
	o.Timestamp = time.Now()
}

func (o *Order) fill(quantity int) {
	o.RemainingQuantity -= quantity
}

func (o *Order) isSettled() bool {
	return o.Event == Fill || o.Event == Cancel
}

type OrderEvent int

const (
	Create OrderEvent = iota
	Update
	Cancel
	PartialFill
	Fill
)

var orderEventMap = map[OrderEvent]string{
	Create:      "Create",
	Update:      "Update",
	Cancel:      "Cancel",
	PartialFill: "PartialFill",
	Fill:        "Fill",
}

func (e OrderEvent) String() string {
	return orderEventMap[e]
}

type OrderType int

const (
	Market OrderType = iota
	Limit
)

var orderTypeMap = map[OrderType]string{
	Market: "Market",
	Limit:  "Limit",
}

func newOrderType() OrderType {
	if rand.Intn(hundredPercent) < marketProbability {
		return Market
	}
	return Limit
}

func (t OrderType) String() string {
	return orderTypeMap[t]
}

type OrderSide int

const (
	Ask OrderSide = iota
	Bid
)

var orderSideMap = map[OrderSide]string{
	Ask: "Ask",
	Bid: "Bid",
}

func (s OrderSide) String() string {
	return orderSideMap[s]
}

func (s OrderSide) opposite() OrderSide {
	return OrderSide(s ^ 1)
}

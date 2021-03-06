package mq

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/gorilla/websocket"
)

var rc *redis.Client

// GlobalChannelMap 全局品种订阅表
var GlobalChannelMap map[BT]*SubChannel

const (
	subscribePrefix = "TICKS:"

	bufSize = 1000
)

var (
	// SUBALL 全部订阅
	SUBALL BT = subscribePrefix + "*"
	// USDCAD 美元
	USDCAD BT = subscribePrefix + "USDCAD"
	// EURCAD 欧元
	EURCAD BT = subscribePrefix + "EURCAD"
)

func init() {
	rc = redis.NewClient(&redis.Options{
		Addr:         ":6379",
		Password:     "",
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
		DB:           0,
	})

	GlobalChannelMap = map[BT]*SubChannel{
		USDCAD: {
			Name:     USDCAD,
			SubCh:    make(chan *redis.Message, bufSize),
			UserList: make(map[string]*WS),
		},
		EURCAD: {
			Name:     EURCAD,
			SubCh:    make(chan *redis.Message, bufSize),
			UserList: make(map[string]*WS),
		},
	}
}

// BT 订阅品种
type BT string

func (b *BT) toString() string {
	return string(*b)
}

// WS 封装Websocket
type WS struct {
	Conn   *websocket.Conn
	closed bool
	sendCh chan []byte
	User
}

// User 用户结构
type User struct {
	userid     int64
	authClient string
	endpoint   string
}

// GetUserKey 获取用户ID
func (ws *WS) GetUserKey() string {
	return fmt.Sprintf("%d:%s:%s", ws.userid, ws.authClient, ws.endpoint)
}

// Send 消费
func (ws *WS) Send(data []byte) {
	if len(ws.sendCh) == cap(ws.sendCh) {
		<-ws.sendCh
	}
	ws.sendCh <- data
}

// Closed 判断连接是否关闭
func (ws *WS) Closed() bool {
	return ws.closed
}

// SubChannel 对应品种的订阅通道和用户连接列表
type SubChannel struct {
	Name  BT
	SubCh chan *redis.Message
	// map userKey WS
	UserList     map[string]*WS
	userListLock sync.Mutex
}

// IsFull 订阅通道已满
func (sc *SubChannel) IsFull() bool {
	return len(sc.SubCh) == cap(sc.SubCh)
}

// RegisterWithLock xx
func (sc *SubChannel) RegisterWithLock(userKey string, w *WS) {
	sc.userListLock.Lock()
	defer sc.userListLock.Unlock()

	old, ok := sc.UserList[userKey]
	if !ok {
		sc.UserList[userKey] = w
		return
	}

	if old.Closed() {
		sc.UserList[userKey] = w
	}
}

// UnRegisterWithLock xx
func (sc *SubChannel) UnRegisterWithLock(userKey string) {
	sc.userListLock.Lock()
	defer sc.userListLock.Unlock()

	delete(sc.UserList, userKey)
}

// UnRegisterAlreadyLocked xx
func (sc *SubChannel) UnRegisterAlreadyLocked(userKey string) {
	delete(sc.UserList, userKey)
}

// Req 用户请求
type Req struct {
	MT     string      `json:"mt"`
	Params interface{} `json:"params,omitempty"`
}

// Subscribe 订阅事件处理
func Subscribe(w *WS, data []byte) error {
	// generate user key
	userKey := w.GetUserKey()

	// parse data into request
	var r Req
	if err := json.Unmarshal(data, &r); err != nil {
		return err
	}

	// register this conn
	params, ok := r.Params.(string)
	if !ok {
		return fmt.Errorf("params should be string type, but got %#v", r.Params)
	}
	var subscribes map[string]struct{}
	switch params {
	case "all", "a", "*":
		for _, subch := range GlobalChannelMap {
			subch.RegisterWithLock(userKey, w)
		}
	default:
		for _, sub := range strings.Split(params, ",") {
			subscribes[strings.TrimSpace(sub)] = struct{}{}
		}
		for kind, subch := range GlobalChannelMap {
			if _, ok := subscribes[kind.toString()]; !ok {
				subch.UnRegisterWithLock(userKey)
			} else {
				subch.RegisterWithLock(userKey, w)
			}
		}
	}
	return nil
}

func subscriberWorker() {
	pubsub := rc.PSubscribe(SUBALL.toString())
	if _, err := pubsub.Receive(); err != nil {
		panic(err)
	}
	defer pubsub.Close()

	channel := pubsub.Channel()
	for msg := range channel {
		kind := BT(msg.Channel)
		if GlobalChannelMap[kind].IsFull() {
			<-GlobalChannelMap[kind].SubCh
		}
		GlobalChannelMap[kind].SubCh <- msg
	}
}

// SubResponse 响应体
type SubResponse struct {
	Bid    float64 `json:"bid"`
	Ask    float64 `json:"ask"`
	Symbol string  `json:"symbol"`
}

func (r *SubResponse) toByte() []byte {
	d, _ := json.Marshal(*r)
	return d
}

func process(data string) []byte {
	var resp SubResponse
	_ = json.Unmarshal([]byte(data), &resp)

	return resp.toByte()
}

func consumerWorker() {
	for kind, sub := range GlobalChannelMap {
		go consume(kind, sub)
	}
}

func consume(k BT, s *SubChannel) {
	for {
		s.userListLock.Lock()
		select {
		case msg := <-s.SubCh:
			data := process(msg.Payload)

			for userKey, userConn := range s.UserList {
				if userConn.Closed() {
					s.UnRegisterAlreadyLocked(userKey)
					continue
				}
				userConn.Send([]byte(data))
			}
		}
		s.userListLock.Unlock()
	}
}

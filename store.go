package ws_hub

import (
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/lesismal/nbio/nbhttp/websocket"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
)

// 定义错误码
var (
	// 连接已丢失
	ErrConnLost error = errors.New("connection lost")
)

// 序列化消息函数
type SerializeFunc func(msg interface{}) []byte

// 发送消息结构体
type sendInfo struct {
	ConnectId string
	Msg       interface{}
}

type ConnStore struct {
	logger zap.Logger
	client map[string]*websocket.Conn
	mu     sync.RWMutex

	sendChan     chan sendInfo
	closeAllChan chan struct{}

	serializeFunc SerializeFunc
}

func NewStore(maxSendWorks int, sendChanSize int, serializeFunc SerializeFunc, logger zap.Logger) *ConnStore {
	res := ConnStore{
		client:        make(map[string]*websocket.Conn),
		mu:            sync.RWMutex{},
		serializeFunc: serializeFunc,
		sendChan:      make(chan sendInfo, sendChanSize),
		closeAllChan:  make(chan struct{}),
		logger:        logger,
	}

	if maxSendWorks <= 0 {
		maxSendWorks = runtime.GOMAXPROCS(0) * 2
	}

	res.startSendWorkers(maxSendWorks)
	return &res
}

func (c *ConnStore) startSendWorkers(maxSendWorks int) {
	for i := 0; i < maxSendWorks; i++ {
		go func() {
			for {
				select {
				case info, ok := <-c.sendChan:
					if !ok {
						c.logger.Info("send chan closed")
						return // sendChan 被关闭
					}
					c.sendMsg(info)
				case <-c.closeAllChan:
					c.logger.Info("close all send workers")
					return // 关闭所有协程
				}
			}
		}()
	}
}

func (c *ConnStore) sendMsg(info sendInfo) {
	c.mu.RLock()
	conn, ok := c.client[info.ConnectId]
	c.mu.RUnlock()
	if !ok {
		c.logger.Warn("connection lost", zap.String("connectId", info.ConnectId))
		return
	}

	msg := c.serializeFunc(info.Msg)

	var err error = nil
	// 重试3次 也可以优化为通过参数传入
	// TODO: 优化为通过参数传入重试次数
	for i := 0; i < 3; i++ {
		err = conn.WriteMessage(websocket.TextMessage, msg)
		if err == nil {
			break
		}
		c.logger.Warn("send message failed", zap.String("connectId", info.ConnectId), zap.Error(err))
		time.Sleep(time.Second * (1 << i)) // 指数退避
	}

	if err != nil {
		c.mu.Lock()
		c.DelConn(info.ConnectId)
		c.mu.Unlock()
	}
}

func (c *ConnStore) AddConn(connect *websocket.Conn) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := ksuid.New().String()
	c.client[id] = connect
	return id
}

func (c *ConnStore) SendMsg(id string, msg interface{}) error {
	c.mu.RLock()
	_, ok := c.client[id]
	c.mu.RUnlock()

	if !ok {
		c.logger.Warn("connection lost", zap.String("connectId", id))
		return ErrConnLost
	}

	c.sendChan <- sendInfo{
		ConnectId: id,
		Msg:       msg,
	}
	return nil
}

func (c *ConnStore) DelConn(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.client[id]; !ok {
		return
	}

	delete(c.client, id)
}

func (c *ConnStore) CloseAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, conn := range c.client {
		conn.Close()
	}

	c.client = make(map[string]*websocket.Conn) // 清空连接
	close(c.closeAllChan)                       // 发送关闭信号给所有协程
	close(c.sendChan)                           // 关闭发送消息通道
}

func (c *ConnStore) Close(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.client[id]; ok {
		conn.Close()
		delete(c.client, id)
	}
}

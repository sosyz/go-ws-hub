package ws_hub

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// 反序列化消息函数
type DeserializeFunc func(data []byte, v interface{}) error

// 消息处理器接口
type MsgHandler interface {
	HandleEvent() ServerEvent
	Process(ctx context.Context, connectId string, msg ServerMsg) (interface{}, error)
}

// 消息中心
type MsgHub struct {
	store *ConnStore

	deserialization DeserializeFunc

	handler sync.Map // 使用sync.Map替代map[ServerEvent]MsgHandler以提高并发安全性

	workerPool *semaphore.Weighted // 控制并发处理消息的数量

	logger zap.Logger
}

func NewMsgHub(store *ConnStore, deserializeFunc DeserializeFunc, maxWorkers int64, logger zap.Logger) *MsgHub {
	return &MsgHub{
		store:           store,
		deserialization: deserializeFunc,
		handler:         sync.Map{},
		workerPool:      semaphore.NewWeighted(maxWorkers),
		logger:          logger,
	}
}

func (h *MsgHub) HandleMessage(ctx context.Context, connectId string, message string) error {
	// 限制同时处理消息的goroutine数量
	if err := h.workerPool.Acquire(ctx, 1); err != nil {
		return err
	}

	go func() {
		defer h.workerPool.Release(1) // 确保资源被释放

		msg := ServerMsg{}
		if err := h.deserialization([]byte(message), &msg); err != nil {
			h.logger.Error("deserialize message failed", zap.String("connectId", connectId), zap.Error(err))
			return
		}

		handler, ok := h.handler.Load(msg.Event)
		if !ok {
			h.logger.Warn("no handler found for event", zap.Uint("event", uint(msg.Event)))
			return
		}

		if _h, ok := handler.(MsgHandler); ok {
			v, err := _h.Process(ctx, connectId, msg)
			if err != nil {
				h.logger.Error("process message failed", zap.String("connectId", connectId), zap.Uint("event", uint(_h.HandleEvent())), zap.Error(err))
				return
			}
			if v != nil {
				h.store.SendMsg(connectId, v)
			}
		} else {
			h.logger.Warn("handler assertion failed", zap.Uint("event", uint(msg.Event)))
		}
	}()

	return nil
}

func (h *MsgHub) RegisterHandler(handler MsgHandler) {
	h.handler.Store(handler.HandleEvent(), handler)
}

func (h *MsgHub) UnregisterHandler(handler MsgHandler) {
	h.handler.Delete(handler.HandleEvent())
}

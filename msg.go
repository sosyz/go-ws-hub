package ws_hub

type ServerEvent uint

type ServerMsg struct {
	Event ServerEvent
}

type ClientEvent uint

const (
	ClientEventUnknown    ClientEvent = iota // 0. 未知事件
	ClientEventBadRequest                    // 1. 请求错误
)

type ClientMsg struct {
	Event ClientEvent
	Msg   string
}

package server

type Event struct {
	Fd      int
	IsTimer bool
	IsRead  bool
	IsError bool
}

const TimerFd = -1

type EventLoop interface {
	Init() error

	RegisterServerSocket(fd int) error

	RegisterClientSocket(fd int) error

	RegisterTimer(intervalMs int) error

	Wait(maxEvents int) ([]Event, error)

	Close() error
}

//go:build darwin

package server

import (
	"errors"
	"fmt"
	"syscall"
)

const kqueueTimerIdent = 0xFFFFFFFF

type KqueueEventLoop struct {
	kqueueFd int
	events   []syscall.Kevent_t
}

func NewEventLoop() EventLoop {
	return &KqueueEventLoop{}
}

func (k *KqueueEventLoop) Init() error {
	fd, err := syscall.Kqueue()
	if err != nil {
		return fmt.Errorf("kqueue creation failed: %w", err)
	}
	k.kqueueFd = fd
	return nil
}

func (k *KqueueEventLoop) RegisterServerSocket(fd int) error {
	event := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}

	_, err := syscall.Kevent(k.kqueueFd, []syscall.Kevent_t{event}, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to register server socket with kqueue: %w", err)
	}
	return nil
}

func (k *KqueueEventLoop) RegisterClientSocket(fd int) error {
	event := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}

	_, err := syscall.Kevent(k.kqueueFd, []syscall.Kevent_t{event}, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to register client socket with kqueue: %w", err)
	}
	return nil
}

func (k *KqueueEventLoop) RegisterTimer(intervalMs int) error {
	timerEvent := syscall.Kevent_t{
		Ident:  kqueueTimerIdent,
		Filter: syscall.EVFILT_TIMER,
		Flags:  syscall.EV_ADD | syscall.EV_ENABLE | syscall.EV_CLEAR,
		Fflags: syscall.NOTE_USECONDS,
		Data:   int64(intervalMs) * 1000,
	}

	_, err := syscall.Kevent(k.kqueueFd, []syscall.Kevent_t{timerEvent}, nil, nil)
	return err
}

func (k *KqueueEventLoop) Wait(maxEvents int) ([]Event, error) {
	if k.events == nil || len(k.events) < maxEvents {
		k.events = make([]syscall.Kevent_t, maxEvents)
	}

	nEvents, err := syscall.Kevent(k.kqueueFd, nil, k.events, nil)
	if err != nil {
		if errors.Is(err, syscall.EBADF) {
			return nil, fmt.Errorf("kqueue closed: %w", err)
		}
		if errors.Is(err, syscall.EINTR) {
			return []Event{}, nil
		}
		return nil, fmt.Errorf("kevent error: %w", err)
	}

	result := make([]Event, nEvents)
	for i := 0; i < nEvents; i++ {
		ev := k.events[i]
		isTimer := ev.Filter == syscall.EVFILT_TIMER
		fd := int(ev.Ident)
		if isTimer {
			fd = TimerFd
		}
		result[i] = Event{
			Fd:      fd,
			IsTimer: isTimer,
			IsRead:  ev.Filter == syscall.EVFILT_READ,
			IsError: ev.Flags&syscall.EV_ERROR != 0,
		}
	}

	return result, nil
}

func (k *KqueueEventLoop) Close() error {
	return syscall.Close(k.kqueueFd)
}

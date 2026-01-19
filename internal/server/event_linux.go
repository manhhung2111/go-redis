//go:build linux

package server

import (
	"errors"
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

type EpollEventLoop struct {
	epollFd int
	timerFd int
	events  []unix.EpollEvent
}

func NewEventLoop() EventLoop {
	return &EpollEventLoop{
		timerFd: -1,
	}
}

func (e *EpollEventLoop) Init() error {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return fmt.Errorf("epoll creation failed: %w", err)
	}
	e.epollFd = fd
	return nil
}

func (e *EpollEventLoop) RegisterServerSocket(fd int) error {
	event := unix.EpollEvent{
		Events: unix.EPOLLIN,
		Fd:     int32(fd),
	}

	if err := unix.EpollCtl(e.epollFd, unix.EPOLL_CTL_ADD, fd, &event); err != nil {
		return fmt.Errorf("failed to register server socket with epoll: %w", err)
	}
	return nil
}

func (e *EpollEventLoop) RegisterClientSocket(fd int) error {
	event := unix.EpollEvent{
		Events: unix.EPOLLIN,
		Fd:     int32(fd),
	}

	if err := unix.EpollCtl(e.epollFd, unix.EPOLL_CTL_ADD, fd, &event); err != nil {
		return fmt.Errorf("failed to register client socket with epoll: %w", err)
	}
	return nil
}

func (e *EpollEventLoop) RegisterTimer(intervalMs int) error {
	timerFd, err := unix.TimerfdCreate(unix.CLOCK_MONOTONIC, unix.TFD_NONBLOCK|unix.TFD_CLOEXEC)
	if err != nil {
		return fmt.Errorf("timerfd creation failed: %w", err)
	}
	e.timerFd = timerFd

	// Convert milliseconds to seconds and nanoseconds
	intervalNs := int64(intervalMs) * 1_000_000
	secs := intervalNs / 1_000_000_000
	nsecs := intervalNs % 1_000_000_000

	spec := unix.ItimerSpec{
		Interval: unix.Timespec{Sec: secs, Nsec: nsecs},
		Value:    unix.Timespec{Sec: secs, Nsec: nsecs},
	}

	if err := unix.TimerfdSettime(e.timerFd, 0, &spec, nil); err != nil {
		unix.Close(e.timerFd)
		e.timerFd = -1
		return fmt.Errorf("timerfd settime failed: %w", err)
	}

	event := unix.EpollEvent{
		Events: unix.EPOLLIN,
		Fd:     int32(e.timerFd),
	}

	if err := unix.EpollCtl(e.epollFd, unix.EPOLL_CTL_ADD, e.timerFd, &event); err != nil {
		unix.Close(e.timerFd)
		e.timerFd = -1
		return fmt.Errorf("failed to register timerfd with epoll: %w", err)
	}

	return nil
}

func (e *EpollEventLoop) Wait(maxEvents int) ([]Event, error) {
	if e.events == nil || len(e.events) < maxEvents {
		e.events = make([]unix.EpollEvent, maxEvents)
	}

	nEvents, err := unix.EpollWait(e.epollFd, e.events, -1)
	if err != nil {
		if errors.Is(err, syscall.EBADF) {
			return nil, fmt.Errorf("epoll closed: %w", err)
		}
		if errors.Is(err, syscall.EINTR) {
			return []Event{}, nil
		}
		return nil, fmt.Errorf("epoll_wait error: %w", err)
	}

	result := make([]Event, nEvents)
	for i := 0; i < nEvents; i++ {
		ev := e.events[i]
		fd := int(ev.Fd)
		isTimer := fd == e.timerFd

		// For timer events, read the timerfd to acknowledge
		if isTimer {
			var buf [8]byte
			unix.Read(e.timerFd, buf[:])
			fd = TimerFd
		}

		result[i] = Event{
			Fd:      fd,
			IsTimer: isTimer,
			IsRead:  ev.Events&unix.EPOLLIN != 0,
			IsError: ev.Events&(unix.EPOLLERR|unix.EPOLLHUP) != 0,
		}
	}

	return result, nil
}

func (e *EpollEventLoop) Close() error {
	if e.timerFd >= 0 {
		unix.Close(e.timerFd)
	}
	return unix.Close(e.epollFd)
}

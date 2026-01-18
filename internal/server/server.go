package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"syscall"

	"github.com/manhhung2111/go-redis/internal/command"
	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/protocol"
)

const activeExpireTimerIdent = 2003

type Server struct {
	redis    command.Redis
	kQueueFd int
	serverFd int
}

func NewServer(redis command.Redis) *Server {
	return &Server{
		redis: redis,
	}
}

func (s *Server) Start(sigCh chan os.Signal) error {
	log.Printf("starting TCP server on %s:%d", config.HOST, config.PORT)

	var err error
	s.serverFd, err = s.createServerSocket()
	if err != nil {
		return fmt.Errorf("failed to create server socket: %w", err)
	}
	defer syscall.Close(s.serverFd)

	s.kQueueFd, err = s.initKqueue()
	if err != nil {
		return fmt.Errorf("failed to initialize kqueue: %w", err)
	}
	defer syscall.Close(s.kQueueFd)

	if err := s.registerServerSocket(); err != nil {
		return fmt.Errorf("failed to register server socket: %w", err)
	}

	if err := s.registerActiveExpireTime(); err != nil {
		return fmt.Errorf("failed to register active expire cycle event: %w", err)
	}

	return s.eventLoop()
}

func (s *Server) createServerSocket() (int, error) {
	serverFD, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return 0, fmt.Errorf("socket creation failed: %w", err)
	}

	if err := syscall.SetNonblock(serverFD, true); err != nil {
		syscall.Close(serverFD)
		return 0, fmt.Errorf("failed to set non-blocking mode: %w", err)
	}

	ipV4 := net.ParseIP(config.HOST)
	if ipV4 == nil {
		syscall.Close(serverFD)
		return 0, fmt.Errorf("invalid IP address: %s", config.HOST)
	}

	sockAddr := &syscall.SockaddrInet4{
		Port: config.PORT,
		Addr: [4]byte{ipV4[0], ipV4[1], ipV4[2], ipV4[3]},
	}

	if err := syscall.Bind(serverFD, sockAddr); err != nil {
		syscall.Close(serverFD)
		return 0, fmt.Errorf("bind to port %d failed: %w", config.PORT, err)
	}

	if err := syscall.Listen(serverFD, config.MAX_CONNECTION); err != nil {
		syscall.Close(serverFD)
		return 0, fmt.Errorf("listen failed: %w", err)
	}

	return serverFD, nil
}

func (s *Server) initKqueue() (int, error) {
	kQueueFd, err := syscall.Kqueue()
	if err != nil {
		return 0, fmt.Errorf("kqueue creation failed: %w", err)
	}
	return kQueueFd, nil
}

func (s *Server) registerServerSocket() error {
	event := syscall.Kevent_t{
		Ident:  uint64(s.serverFd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}

	_, err := syscall.Kevent(s.kQueueFd, []syscall.Kevent_t{event}, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to register server socket with kqueue: %w", err)
	}
	return nil
}

func (s *Server) registerActiveExpireTime() error {
	timerEvent := syscall.Kevent_t{
		Ident:  uint64(activeExpireTimerIdent),
		Filter: syscall.EVFILT_TIMER,
		Flags:  syscall.EV_ADD | syscall.EV_ENABLE | syscall.EV_CLEAR,
		Fflags: syscall.NOTE_USECONDS, //
		Data:   int64(config.ACTIVE_EXPIRE_CYCLE_MS) * 1000,
	}

	_, err := syscall.Kevent(s.kQueueFd, []syscall.Kevent_t{timerEvent}, nil, nil)
	return err
}

func (s *Server) eventLoop() error {
	events := make([]syscall.Kevent_t, config.MAX_CONNECTION)

	for {
		nEvents, err := syscall.Kevent(s.kQueueFd, nil, events, nil)
		if err != nil {
			if errors.Is(err, syscall.EBADF) {
				log.Println("kqueue closed, shutting down")
				break
			}
			log.Printf("kevent error: %v", err)
			continue
		}

		for i := 0; i < nEvents; i++ {
			if err := s.handleEvent(events[i]); err != nil {
				log.Printf("error handling event: %v", err)
			}
		}
	}

	log.Println("server shutdown complete")
	return nil
}

func (s *Server) handleEvent(event syscall.Kevent_t) error {
	if int(event.Ident) == activeExpireTimerIdent && event.Filter == syscall.EVFILT_TIMER {
		s.redis.ActiveExpireCycle()
		return nil
	}

	if int(event.Ident) == s.serverFd {
		return s.acceptConnection()
	}
	return s.handleClientRequest(int(event.Ident))
}

func (s *Server) acceptConnection() error {
	connFD, _, err := syscall.Accept(s.serverFd)
	if err != nil {
		return fmt.Errorf("accept failed: %w", err)
	}

	if err := syscall.SetNonblock(connFD, true); err != nil {
		syscall.Close(connFD)
		return fmt.Errorf("failed to set client socket to non-blocking: %w", err)
	}

	clientReadEvent := syscall.Kevent_t{
		Ident:  uint64(connFD),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}

	if _, err := syscall.Kevent(s.kQueueFd, []syscall.Kevent_t{clientReadEvent}, nil, nil); err != nil {
		syscall.Close(connFD)
		return fmt.Errorf("failed to register client socket: %w", err)
	}

	return nil
}

func (s *Server) handleClientRequest(clientFD int) error {
	buf := make([]byte, 512)
	n, err := syscall.Read(clientFD, buf)
	if err != nil {
		syscall.Close(clientFD)
		return fmt.Errorf("read from client failed: %w", err)
	}

	if n == 0 {
		syscall.Close(clientFD)
		return nil
	}

	cmd, err := protocol.ParseCmd(buf[:n])
	var response []byte
	if err != nil {
		response = protocol.EncodeResp(err, false)
	} else {
		response = s.redis.HandleCommand(*cmd)
	}

	if _, err := syscall.Write(clientFD, response); err != nil {
		syscall.Close(clientFD)
		return fmt.Errorf("write to client failed: %w", err)
	}

	return nil
}

func (s *Server) WaitingForSignals(sigCh chan os.Signal) {
	<-sigCh
	log.Println("shutdown signal received")
	syscall.Close(s.kQueueFd)
}

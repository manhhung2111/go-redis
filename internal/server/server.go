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

type Server struct {
	config    *config.Config
	redis     command.Redis
	eventLoop EventLoop
	serverFd  int
}

func NewServer(cfg *config.Config, redis command.Redis) *Server {
	return &Server{
		config: cfg,
		redis:  redis,
	}
}

func (s *Server) Start(sigCh chan os.Signal) error {
	log.Printf("starting TCP server on %s:%d", s.config.Host, s.config.Port)

	var err error
	s.serverFd, err = s.createServerSocket()
	if err != nil {
		return fmt.Errorf("failed to create server socket: %w", err)
	}
	defer syscall.Close(s.serverFd)

	s.eventLoop = NewEventLoop()
	if err := s.eventLoop.Init(); err != nil {
		return fmt.Errorf("failed to initialize event loop: %w", err)
	}
	defer s.eventLoop.Close()

	if err := s.eventLoop.RegisterServerSocket(s.serverFd); err != nil {
		return fmt.Errorf("failed to register server socket: %w", err)
	}

	if err := s.eventLoop.RegisterTimer(s.config.ActiveExpireCycleMs); err != nil {
		return fmt.Errorf("failed to register active expire cycle event: %w", err)
	}

	return s.runEventLoop()
}

func (s *Server) createServerSocket() (int, error) {
	serverFD, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return 0, fmt.Errorf("socket creation failed: %w", err)
	}

	// Allow quick port reuse after server restart
	if err := syscall.SetsockoptInt(serverFD, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		syscall.Close(serverFD)
		return 0, fmt.Errorf("failed to set SO_REUSEADDR: %w", err)
	}

	if err := syscall.SetNonblock(serverFD, true); err != nil {
		syscall.Close(serverFD)
		return 0, fmt.Errorf("failed to set non-blocking mode: %w", err)
	}

	ipV4 := net.ParseIP(s.config.Host)
	if ipV4 == nil {
		syscall.Close(serverFD)
		return 0, fmt.Errorf("invalid IP address: %s", s.config.Host)
	}

	sockAddr := &syscall.SockaddrInet4{
		Port: s.config.Port,
		Addr: [4]byte{ipV4[0], ipV4[1], ipV4[2], ipV4[3]},
	}

	if err := syscall.Bind(serverFD, sockAddr); err != nil {
		syscall.Close(serverFD)
		return 0, fmt.Errorf("bind to port %d failed: %w", s.config.Port, err)
	}

	if err := syscall.Listen(serverFD, s.config.MaxConnection); err != nil {
		syscall.Close(serverFD)
		return 0, fmt.Errorf("listen failed: %w", err)
	}

	return serverFD, nil
}

func (s *Server) runEventLoop() error {
	for {
		events, err := s.eventLoop.Wait(s.config.MaxConnection)
		if err != nil {
			if errors.Is(err, syscall.EBADF) {
				log.Println("event loop closed, shutting down")
				break
			}
			log.Printf("event loop error: %v", err)
			continue
		}

		for _, event := range events {
			if err := s.handleEvent(event); err != nil {
				log.Printf("error handling event: %v", err)
			}
		}
	}

	log.Println("server shutdown complete")
	return nil
}

func (s *Server) handleEvent(event Event) error {
	if event.IsTimer {
		s.redis.ActiveExpireCycle()
		return nil
	}

	if event.Fd == s.serverFd {
		return s.acceptConnection()
	}
	return s.handleClientRequest(event.Fd)
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

	if err := s.eventLoop.RegisterClientSocket(connFD); err != nil {
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
	s.eventLoop.Close()
}

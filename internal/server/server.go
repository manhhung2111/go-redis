package server

import (
	"errors"
	"log"
	"net"
	"os"
	"syscall"

	"github.com/manhhung2111/go-redis/internal/command"
	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/core"
)

type Server struct {
	redis  command.Redis
	kQueueFd int
}

func NewServer(
	redis command.Redis,
) *Server {
	return &Server{
		redis:  redis,
	}
}

func (server *Server) Start(sigCh chan os.Signal) error {
	log.Println("starting a TCP server listening on", config.HOST, config.PORT)

	var events []syscall.Kevent_t = make([]syscall.Kevent_t, config.MAX_CONNECTION)
	// Create a socket listening for new connections
	serverFD, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		log.Printf("error occurred when trying to create a socket listening for new connections. Err: %v \n", err.Error())
		return err
	}
	defer syscall.Close(serverFD)

	if err = syscall.SetNonblock(serverFD, true); err != nil {
		return err
	}

	// Bind the IP and the port to the server socket FD.
	ipV4 := net.ParseIP(config.HOST)
	if err = syscall.Bind(serverFD, &syscall.SockaddrInet4{
		Port: config.PORT,
		Addr: [4]byte{ipV4[0], ipV4[1], ipV4[2], ipV4[3]},
	}); err != nil {
		log.Printf("error occurred when trying to bind PORT %v to server socket. Err: %v \n", config.PORT, err.Error())
		return err
	}

	// Start listening
	if err = syscall.Listen(serverFD, config.MAX_CONNECTION); err != nil {
		log.Printf("error occurred when trying to listen socket server. Err: %v \n", err.Error())
		return err
	}

	// A kernel event queue used to register and receive readiness notifications for other FDs
	kQueueFd, err := syscall.Kqueue()
	if err != nil {
		log.Fatal("error occurred when trying to create a new Kqueue instance", err)
	}
	server.kQueueFd = kQueueFd
	defer syscall.Close(kQueueFd)

	// Specify the events we want to monitor server socket FD, in here we are interested in READ event
	var socketServerReadyEvent syscall.Kevent_t = syscall.Kevent_t{
		Ident:  uint64(serverFD),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}

	syscall.Kevent(kQueueFd, []syscall.Kevent_t{socketServerReadyEvent}, nil, nil)

	for {
		// block the main thread until one or more registered events become ready, then copy them into `events`
		nEvents, err := syscall.Kevent(kQueueFd, nil, events, nil)
		if err != nil {
			if errors.Is(err, syscall.EBADF) {
				break
			}
			continue
		}

		for i := 0; i < nEvents; i++ {
			// if the socket server itself is ready for an IO
			if int(events[i].Ident) == serverFD {
				connFD, _, err := syscall.Accept(serverFD)
				if err != nil {
					log.Print("error occurred when trying to accept a new client connection, err=", err)
					continue
				}

				if err = syscall.SetNonblock(connFD, true); err != nil {
					return err
				}

				clientReadEvent := syscall.Kevent_t{
					Ident:  uint64(connFD),
					Filter: syscall.EVFILT_READ,
					Flags:  syscall.EV_ADD,
				}

				if _, err = syscall.Kevent(kQueueFd, []syscall.Kevent_t{clientReadEvent}, nil, nil); err != nil {
					log.Fatal(err)
				}
			} else {
				comm := core.FDComm{Fd: int(events[i].Ident)}
				cmd, err := readCommandFD(comm)
				if err != nil {
					syscall.Close(int(events[i].Ident))
					continue
				}
				response := server.redis.HandleCommand(*cmd)
				comm.Write(response)
			}
		}
	}

	syscall.Close(serverFD)
	log.Println("server shutdown complete")
	return nil
}

func (server *Server) WaitingForSignals(sigCh chan os.Signal) {
	<-sigCh
	log.Println("shutdown signal received")
	syscall.Close(server.kQueueFd)
}

func readCommandFD(comm core.FDComm) (*core.RedisCmd, error) {
	var buf []byte = make([]byte, 512)
	n, err := comm.Read(buf)
	if err != nil {
		return nil, err
	}
	return core.ParseCmd(buf[:n])
}

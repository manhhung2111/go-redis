package server

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/manhhung2111/go-redis/internal/command"
	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)

type Server struct {
	redis command.Redis
}

func NewServer(
	redis command.Redis,
) *Server {
	return &Server{
		redis: redis,
	}
}

func (server *Server) Start() error {
	log.Println("starting a TCP server listening on", config.HOST, config.PORT)

	var events []syscall.Kevent_t = make([]syscall.Kevent_t, config.MAX_CONNECTION)
	clients := 0

	// Create a socket listening for new connections
	serverFD, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
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
		return err
	}

	// Start listening
	if err = syscall.Listen(serverFD, config.MAX_CONNECTION); err != nil {
		return err
	}

	// A kernel event queue used to register and receive readiness notifications for other FDs
	kQueueFd, err := syscall.Kqueue()
	if err != nil {
		log.Fatal("error occurred when trying to create a new Kqueue instance", err)
	}
	defer syscall.Close(kQueueFd)

	// Specify the events we want to monitor server socket FD, in here we are interested in READ event
	var socketServerReadyEvent syscall.Kevent_t = syscall.Kevent_t{
		Ident:  uint64(serverFD),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}

	syscall.Kevent(kQueueFd, []syscall.Kevent_t{socketServerReadyEvent}, nil, nil)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("shutting down server...")
		syscall.Close(serverFD)
		syscall.Close(kQueueFd)
		os.Exit(0)
	}()

	for {
		// block the main thread until one or more registered events become ready, then copy them into `events`
		nEvents, err := syscall.Kevent(kQueueFd, nil, events, nil)
		if err != nil {
			continue
		}

		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

		for i := 0; i < nEvents; i++ {
			// if the socket server itself is ready for an IO
			if int(events[i].Ident) == serverFD {
				clients++
				log.Printf("new client: id=%d\n", clients)

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
					clients--
					continue
				}
				response := server.handleCommand(*cmd)
				comm.Write(response)
			}
		}
	}
}

func readCommandFD(comm core.FDComm) (*core.RedisCmd, error) {
	var buf []byte = make([]byte, 512)
	n, err := comm.Read(buf)
	if err != nil {
		return nil, err
	}
	return core.ParseCmd(buf[:n])
}

func (server *Server) handleCommand(cmd core.RedisCmd) []byte {
	switch cmd.Cmd {
	case "PING":
		return server.redis.Ping(cmd)
	case "SET":
		return server.redis.Set(cmd)
	case "GET":
		return server.redis.Get(cmd)
	case "DEL":
		return server.redis.Del(cmd)
	case "TTL":
		return server.redis.TTL(cmd)
	case "EXPIRE":
		return server.redis.Expire(cmd)
	case "INCR":
		return server.redis.Incr(cmd)
	case "INCRBY":
		return server.redis.IncrBy(cmd)
	case "DECR":
		return server.redis.Decr(cmd)
	case "DECRBY":
		return server.redis.DecrBy(cmd)
	case "MGET":
		return server.redis.MGet(cmd)
	case "MSET":
		return server.redis.MSet(cmd)
	}
	return core.EncodeResp(util.InvalidCommand(cmd.Cmd), false)
}

package protocol

type RedisCmd struct {
	Cmd  string
	Args []string
}
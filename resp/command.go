package resp

type Command string

const (
	Ping Command = "PING"
	Echo Command = "ECHO"
	Set  Command = "SET"
	Get  Command = "GET"
)

package MapReduce

import (
	"database/sql"
	"fmt"
	//"github.com/mattn/go-sqlite3"
	"log"
	//"bufio"
	//"time"
	"errors"
	"strings"
	//"os"
	"net"
	"net/http"
	"net/rpc"
	"runtime"
	"strconv"
)

var Global_Chat_Level int

//these are used in task type
const (
	WORK_DONE   = "1"
	WORK_MAP    = "2"
	WORK_REDUCE = "3"
	WAIT        = "4"
)
const (
	FULL_DEBUG   = 0 //shows everything in loops
	VARS_DEBUG   = 1 //shows all variables
	ERRO_DEBUG   = 2 //only shows errors
	MESSAGES     = 3 //messages like "task assigned" "task complete" etc.
	SPECIAL_CASE = 4 //rare cases that need to be shown no matter what
)

type Config struct {
	MasterIP       string
	InputData      string
	OutputFileName string
	MapperNum      int
	ReducerNum     int
	TableName      string
}
type Task struct {
	//id of worker who is working on it
	WorkerID int
	//type of work, see consts
	Type int
	//file name of sqlite file
	Filename string
	//starting row
	Offset int
	//number of rows to work
	Size int
	//number of mappers
	NumMappers  int
	NumReducers int
	//table name to work on
	Table string
	//evrybody in the quorum
	MapAddresses []string
}
type Server struct {
	NumMappers  int
	NumReducers int
	Tasks       []Task
	Address     string
	MaxServers  int
	//base ip for building an ip in GetLocalAddress
	//will default to :3410
	StartingIP  int
	IsListening bool
}

//This will be our struct to hold the data supplied by russ
type Pair struct {
	Key   string
	Value string
}
type PingResponse struct {
	ResponderAddress string
	Responded        bool
}

/*
	Choose a desired level using the consts
	Any level lower than the global will be printed
*/
func LogF(level int, message string, args ...interface{}) {
	if level >= Global_Chat_Level || level == SPECIAL_CASE {
		log.Println(fmt.Sprintf(message, args...))
	}
}

/*
	line number where error was called,
	the name of function this was called in,
	the message with printf flags,
	the args for printf
*/
func PrintError(err error) {
	if VARS_DEBUG >= Global_Chat_Level {
		log.Println(err)
	}
}

/*
	Special error format:

	skip = number of functions to skip in the stack.
	eg. stack goes, Server.Create() -> SetLocalAddress() -> FormatError()
	skip 0 will get SetLocalAddress()
	skip 1 will get Server.Create()

	message and args are standard printf format
*/
func FormatError(skip int, message string, args ...interface{}) error {
	message = fmt.Sprintf(message, args...)
	callerFunc, callerFile, callerLine, okay := runtime.Caller(skip)
	if !okay {
		LogF(SPECIAL_CASE, "Could not trace stack in an error report")
		return nil
	}
	functionName := runtime.FuncForPC(callerFunc).Name()
	return errors.New(fmt.Sprintf("\n    Error ---> %s \n    %s\n %s : %d ", functionName, message, callerFile, callerLine))
}
func NewServer(chatLevel int, configSettings) Server {
	Global_Chat_Level = chatLevel
	var tempServer Server
	tempServer.NumMappers = 3
	tempServer.NumReducers = 3
	// max servers is workers plus worker
	// this is only used when grabbing an IP
	tempServer.MaxServers = tempServer.NumMappers + tempServer.NumReducers + 1
	tempServer.StartingIP = 3410
	tempServer.IsListening = false
	tempServer.SetLocalAddress(tempServer.StartingIP)
	tempServer.Tasks = make([]Task, 1000)
	tempServer.create()
	return tempServer
}

/*
	Takes an integer representing the desired port
	And converts is into a valid address string for
	RPC.register()
*/
func PortIntToAddressString(intPort int) (string, error) {
	tempPort := ":" + strconv.Itoa(intPort)
	stringPort := CheckAddressValidity(tempPort)
	if stringPort == "" {
		err := errors.New("Failed to convert port to string")
		return stringPort, err
	}
	return stringPort, nil
}

/*
	Checks to make sure string is valid IP
	If string is just a port, then it will append localhost
*/
func CheckAddressValidity(s string) string {
	if strings.HasPrefix(s, ":") {
		return "127.0.0.1" + s
	} else if strings.Contains(s, ":") {
		return s
	} else {
		return ""
	}
}
func (elt *Pair) QuerySQLFromStructKey(database *sql.DB) (ReturnValue string) {
	rows, err := database.Query("SELECT value FROM Pairs WHERE key=?", elt.Key)
	if err != nil {
		return "Could not Query Database in Pair.QuerySQLFromStruct()"
	}
	for rows.Next() {
		var value string
		if err2 := rows.Scan(&value); err2 != nil {
			return fmt.Sprintf("Could not read rows in Pair.QuerySQLFromStructKey: %v\n", err2)
		}
		value = fmt.Sprintf("	Key:  %s\n	Value:  %s\n", elt.Key, value)
		//strings.Join(ReturnValue, value)
		ReturnValue = fmt.Sprintf("%s\n%s", ReturnValue, value)
	}
	return ReturnValue
}
func (elt *Pair) InsertSQL(database *sql.DB) error {
	_, err := database.Exec("INSERT INTO Pairs (ID, key, value) VALUES (?, ?, ?)", nil, elt.Key, elt.Value)
	return err
}

/*
	Returns whatever
	Server.Address is
*/
func (elt *Server) GetLocalAddress() string {
	return elt.Address
}

/*
	Only makes changes to the listener if
	this function is called BEFORE Server.create()
*/
func (elt *Server) SetLocalAddress(newAddressInt int) error {
	if elt.IsListening {
		return FormatError(1, "Server already listening on: [%s]", elt.GetLocalAddress())
	}
	tempAddress, err := PortIntToAddressString(newAddressInt)
	if err != nil {
		return FormatError(1, "Not a valid port number [%d] \n    Error: [%v]", newAddressInt, err)
	} else {
		elt.Address = tempAddress
		return nil
	}
}
func (elt *Server) Ping(sender string, reply *PingResponse) error {
	log.Println("Ping from : ", sender)
	reply.Responded = true
	reply.ResponderAddress = elt.GetLocalAddress()
	return nil
}
func (elt *Server) create() error {
	rpc.Register(elt)
	rpc.HandleHTTP()
	listening := false
	nextAddress := 0
	var l net.Listener
	for !listening {
		LogF(VARS_DEBUG, "Trying address: [%s]", elt.GetLocalAddress())
		nextAddress += 1
		listener, err := net.Listen("tcp", elt.GetLocalAddress())
		if err != nil {
			if nextAddress >= elt.MaxServers {
				log.Fatal("Map Recuce is full")
			}
			//build next IP
			if err := elt.SetLocalAddress(elt.StartingIP + nextAddress); err != nil {
				PrintError(err)
			}
		} else {
			l = listener
			listening = true
		}
	}
	LogF(MESSAGES, "Address is: [%s]", elt.GetLocalAddress())
	go http.Serve(l, nil)
	return nil
}

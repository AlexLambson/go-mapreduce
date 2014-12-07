package MapReduce

import (
	//"database/sql"
	"fmt"
	//"github.com/mattn/go-sqlite3"
	"log"
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

/*
	Log Level constants:
	used in Logf
*/
const (
	FULL_DEBUG   = 0 //shows everything in loops
	VARS_DEBUG   = 1 //shows all variables
	ERRO_DEBUG   = 2 //only shows errors
	MESSAGES     = 3 //messages like "task assigned" "task complete" etc.
	SPECIAL_CASE = 4 //rare cases that need to be shown no matter what
)

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
	Used to allow errors to be shown/hidden using
	global debug levels without the need to type
	---- Logf(VARS_DEBUG, "%v", errorVar)
	after every if err != nil
*/
func PrintError(err error) {
	if VARS_DEBUG >= Global_Chat_Level {
		log.Println(err)
	}
}

/*
	Special error format:

	skip = number of functions to skip in the stack.
	eg. stack goes, MasterServer.Create() -> SetLocalAddress() -> FormatError()
	skip 0 will get SetLocalAddress()
	skip 1 will get MasterServer.Create()

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

/*
	Called in MapReduce.StartMaster()
	Handles creating and configuring
	a master server struct
*/
func NewMasterServer(LogLevel int, configSettings Config) MasterServer {
	Global_Chat_Level = LogLevel
	var self MasterServer
	self.NumMappers = 3
	self.NumReducers = 3
	// max servers is workers plus worker
	// this is only used when grabbing an IP
	self.MaxServers = self.NumMappers + self.NumReducers + 1
	self.StartingIP = 3410
	self.IsListening = false
	self.SetLocalAddress(self.StartingIP)
	self.Tasks = make([]Task, 1000)
	self.listen()
	return self
}
func (elt *MasterServer) create() error {
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

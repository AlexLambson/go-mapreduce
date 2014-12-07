/*
	******************************************************
	Anything master-related that does not deal with the
	actual map reduce master algorithim goes in this file
	******************************************************
*/

package mapreduce

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
	eg. stack goes, MasterServer.Create() -> SetServerAddress() -> FormatError()
	skip 0 will get SetServerAddress()
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
func NewMasterServer(Settings Config, Tasks *[]Task) MasterServer {
	Global_Chat_Level = Settings.LogLevel
	var self MasterServer
	self.NumMapTasks = Settings.NumReduceTasks
	self.NumReduceTasks = Settings.NumReduceTasks
	// max servers is workers plus worker
	// this is only used when grabbing an IP
	self.MaxServers = Settings.NumMapTasks*2 + Settings.NumReduceTasks*2
	self.StartingIP = 3410
	self.IsListening = false
	self.SetServerAddress(self.StartingIP)
	self.Tasks = *Tasks
	self.listen()
	return self
}

func Extend(array []interface{}, element interface{}, sizeMultiplier int) []interface{} {
	length := len(array)
	if length == cap(array) {
		// Slice is full; must grow.
		// We double its size and add 1, so if the size is zero we still grow.
		newArray := make([]interface{}, length, sizeMultiplier*length+1)
		copy(newArray, array)
		array = newArray
	}
	array[length] = element
	return array
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

/*
	Returns whatever
	MasterServer.Address is
*/
func (elt *MasterServer) GetServerAddress() string {
	return elt.Address
}

/*
	Only makes changes to the listener if
	this function is called BEFORE MasterServer.create()
*/
func (elt *MasterServer) SetServerAddress(newAddressInt int) error {
	if elt.IsListening {
		return FormatError(1, "MasterServer already listening on: [%s]", elt.GetServerAddress())
	}
	tempAddress, err := PortIntToAddressString(newAddressInt)
	if err != nil {
		return FormatError(1, "Not a valid port number [%d] \n    Error: [%v]", newAddressInt, err)
	} else {
		elt.Address = tempAddress
		return nil
	}
}

/*
	Register the master on rpc
	then serve it and listen on the starting port
*/
func (elt *MasterServer) listen() error {
	rpc.Register(elt)
	rpc.HandleHTTP()
	listening := false
	nextAddress := 0
	var l net.Listener
	for !listening {
		LogF(VARS_DEBUG, "Trying address: [%s]", elt.GetServerAddress())
		nextAddress += 1
		listener, err := net.Listen("tcp", elt.GetServerAddress())
		if err != nil {
			if nextAddress >= elt.MaxServers {
				log.Fatal("Map Recuce is full")
			}
			//build next IP
			if err := elt.SetServerAddress(elt.StartingIP + nextAddress); err != nil {
				PrintError(err)
			}
		} else {
			l = listener
			listening = true
		}
	}
	LogF(MESSAGES, "Address is: [%s]", elt.GetServerAddress())
	go http.Serve(l, nil)
	return nil
}
func findOpenIP(StartingIP int) (openAddress string, l net.Listener) {
	//Only used for it's functions
	var elt MasterServer
	elt.SetServerAddress(StartingIP)
	foundPort := false
	nextPort := 0
	for !foundPort {
		LogF(VARS_DEBUG, "Trying address: [%s]", elt.GetServerAddress())
		nextPort += 1
		listener, err := net.Listen("tcp", elt.GetServerAddress())
		if err != nil {
			//build next IP
			if err := elt.SetServerAddress(StartingIP + nextPort); err != nil {
				PrintError(err)
			}
		} else {
			l = listener
			foundPort = true
		}
	}
	openAddress = elt.GetServerAddress()
	return openAddress, l
}

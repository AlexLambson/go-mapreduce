/*
	******************************************************
	Anything master-related that does not deal with the
	actual map reduce master algorithim goes in this file
	******************************************************
*/

package mapreduce

import (
	"database/sql"
	"fmt"
	//"github.com/mattn/go-sqlite3"
	"log"
	//"time"
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"runtime"
	"strconv"
	"strings"
)

/*
	Type declarations for functions
*/
type MapFunction func(key, value string, output chan<- Pair) error
type ReduceFunction func(key string, values <-chan string, output chan<- Pair) error

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
	self.ReduceCount = 0
	self.DoneChannel = make(chan int)
	self.Table = Settings.TableName
	self.Output = Settings.OutputFolderName
	self.LogLevel = Settings.LogLevel
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
			LogF(ERRO_DEBUG, "%v", err)
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
func FindOpenIP(StartingIP int) (openAddress string) {
	//Only used for it's functions
	var elt MasterServer
	var l net.Listener
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
	l.Close()
	openAddress = elt.GetServerAddress()
	return openAddress
}
func Merge(ReduceTasks int, reduceFunction ReduceFunction, output string) error {
	os.Mkdir("/tmp/AK47", 1777)
	// Combine all the rows into a single input file
	sqlCommands := []string{
		"create table if not exists data (key text not null, value text not null)",
		"create index if not exists data_key on data (key asc, value asc);",
		"pragma synchronous = off;",
		"pragma journal_mode = off;",
	}
	for i := 0; i < ReduceTasks; i++ {

		LogF(VARS_DEBUG, "Aggregating Reducer Output Files")

		db, err := sql.Open("sqlite3", fmt.Sprintf("%s/reduce_out_%d.sql", output, i))
		if err != nil {
			log.Println(err)
			continue
		}
		defer db.Close()

		rows, err := db.Query("select key, value from data;")
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer rows.Close()

		for rows.Next() {
			var key string
			var value string
			rows.Scan(&key, &value)
			sqlCommands = append(sqlCommands, fmt.Sprintf("insert into data values ('%s', '%s');", key, value))
		}
	}

	enddb, err := sql.Open("sqlite3", "/tmp/AK47/end.sql")
	for _, sql := range sqlCommands {
		_, err = enddb.Exec(sql)
		if err != nil {
			LogF(ERRO_DEBUG, "%q: %s\n", err, sql)
		}
	}
	enddb.Close()

	enddb, err = sql.Open("sqlite3", ("/tmp/AK47/end.sql"))
	defer enddb.Close()
	rows, err := enddb.Query("select key, value from data order by key asc;")
	if err != nil {
		LogF(ERRO_DEBUG, "sql.Query3\n%v", err)
		return err
	}
	defer rows.Close()

	var key string
	var value string
	rows.Next()
	rows.Scan(&key, &value)

	inChan := make(chan string)
	outChan := make(chan Pair)
	go func() {
		err = reduceFunction(key, inChan, outChan)
		if err != nil {
			PrintError(err)
		}
	}()
	inChan <- value
	current := key

	var outputPairs []Pair
	// Walk through the file's rows, performing the reduce func
	for rows.Next() {
		rows.Scan(&key, &value)
		if key == current {
			inChan <- value
		} else {
			close(inChan)
			p := <-outChan
			outputPairs = append(outputPairs, p)

			inChan = make(chan string)
			outChan = make(chan Pair)
			go func() {
				err = reduceFunction(key, inChan, outChan)
				if err != nil {
					PrintError(err)
				}
			}()
			inChan <- value
			current = key
		}
	}
	close(inChan)
	p := <-outChan
	outputPairs = append(outputPairs, p)

	// Prepare tmp database
	dbfin, err := sql.Open("sqlite3", fmt.Sprintf("%s/output.sql", output))
	defer dbfin.Close()
	if err != nil {
		PrintError(FormatError(0, "Failed in opening final output:\n%v", err))
		return err
	}
	sqlCommands = []string{
		"create table if not exists data (key text not null, value text not null)",
		"create index if not exists data_key on data (key asc, value asc);",
		"pragma synchronous = off;",
		"pragma journal_mode = off;",
	}
	for _, sql := range sqlCommands {
		_, err = dbfin.Exec(sql)
		if err != nil {
			LogF(ERRO_DEBUG, "%q: %s\n", err, sql)
			return err
		}
	}

	// Write the data locally
	for _, outputPair := range outputPairs {
		sql := fmt.Sprintf("insert into data values ('%s', '%s');", outputPair.Key, outputPair.Value)
		_, err = dbfin.Exec(sql)
		if err != nil {
			LogF(ERRO_DEBUG, "%q: %s\n", err, sql)
			return err
		}
	}
	return nil
}

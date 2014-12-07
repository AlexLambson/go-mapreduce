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
	"strconv"
)

var Global_Chat_Level int

/*
	Task-type constants
*/
const (
	TASK_DONE   = "1"
	TASK_MAP    = "2"
	TASK_REDUCE = "3"
	SLEEP       = "4"
)

type Config struct {
	MasterIP         string //IP of the master server
	InputData        string //file location of the sqlite3 file to be worked
	OutputFolderName string //foler location for final output and reduce output
	MapTasksNumber   int    //max number of map tasks
	ReduceTaksNumber int    //max number of reduce tasks
	TableName        string //Table name to pull data from
	LogLevel         int    //Controls the amount of log messages, see the consts
	StartingIP       int
}
type Task struct {
	WorkerID        int      //id of worker who is working on it
	Type            int      //type of work, see consts
	Filename        string   //file name of sqlite file
	Offset          int      //starting row
	Size            int      //number of rows to work
	NumMappers      int      //number of mappers
	NumReducers     int      //number of reducers
	Table           string   //table name to work on
	WorkerAddresses []string //evrybody in the quorum
}
type MasterServer struct {
	NumMappers       int
	NumReducers      int
	Tasks            []Task   //Tasks to be performed
	Address          string   //my ip. only set with MasterServer.SetLocalAddress() because code relies on a fully qualified ip
	MaxServers       int      //max number of IPs to try before giving up. Used to prevent an infinite loop in MasterServer.create()
	StartingIP       int      //base ip for building an ip in GetLocalAddress. will default to :3410
	IsListening      bool     //has this server registered rpc?
	NumTasksAssigned int      //number of map assignments that have been handed out
	WorkerAddresses  []string //addresses of whole quorum
	/*
		MapDoneCount int
		ReduceCount  int
		WorkDone     int
		DoneChan     chan int
		Merged       bool
		Table        string
		Output       string
		Verb         bool
	*/
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

func NewMasterRPC(LogLevel int, configSettings Config) MasterServer {
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
	self.create()
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
	MasterServer.Address is
*/
func (elt *MasterServer) GetLocalAddress() string {
	return elt.Address
}

/*
	Only makes changes to the listener if
	this function is called BEFORE MasterServer.create()
*/
func (elt *MasterServer) SetLocalAddress(newAddressInt int) error {
	if elt.IsListening {
		return FormatError(1, "MasterServer already listening on: [%s]", elt.GetLocalAddress())
	}
	tempAddress, err := PortIntToAddressString(newAddressInt)
	if err != nil {
		return FormatError(1, "Not a valid port number [%d] \n    Error: [%v]", newAddressInt, err)
	} else {
		elt.Address = tempAddress
		return nil
	}
}
func (elt *MasterServer) Ping(sender string, reply *PingResponse) error {
	log.Println("Ping from : ", sender)
	reply.Responded = true
	reply.ResponderAddress = elt.GetLocalAddress()
	return nil
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

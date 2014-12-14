package mapreduce

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"math"
	"runtime"
	//"bufio"
	"io/ioutil"
	"time"
	//"errors"
	//"strings"
	"os"
	//"net"
	"net/http"
	//"net/rpc"
	//"strconv"
)

var Global_Chat_Level int

/*
	Task-type constants
*/
const (
	TASK_DONE   = 1
	TASK_MAP    = 2
	TASK_REDUCE = 3
	SLEEP       = 4
	DOWNLOAD    = 5
	STANDBY     = 6
)

type Request struct {
	Message string
	Address string //myaddress
	Type    int    //get from consts
	Task    Task
}

type Response struct {
	Message      string
	DatabaseName string
	StartingRow  int //offset
	RowsToWork   int //chunksize
	Type         int //pick from
	Mapper       int
	Reducer      int
	Task         Task
	Output       string
	StartingIP   int
}
type Config struct {
	MasterIP         string //IP of the master server
	InputFileName    string //relative file name of the sqlite3 file to be worked
	OutputFolderName string //relative foler name for final output and reduce output
	NumMapTasks      int    //max number of map tasks
	NumReduceTasks   int    //max number of reduce tasks
	TableName        string //Table name to pull data from
	LogLevel         int    //Controls the amount of log messages, see the consts
	StartingIP       int    //The port to start on when looking for an IP
}
type Task struct {
	WorkerID         int      //id of worker who completed the map task
	Type             int      //type of work, see consts
	Filename         string   //file name of sqlite file
	Offset           int      //starting row
	Size             int      //number of rows to work
	NumMapTasks      int      //number of map tasks
	NumReducers      int      //number of reduce tasks
	Table            string   //table name to work on
	MapFileLocations []string //locations of the files created by a mapper
}
type MasterServer struct {
	NumMapTasks      int      //Max number of map tasks
	NumReduceTasks   int      //Max number of reduce tasks
	Tasks            []Task   //Tasks to be performed
	Address          string   //my ip. only set with MasterServer.SetServerAddress() because code relies on a fully qualified ip
	MaxServers       int      //max number of IPs to try before giving up. Used to prevent an infinite loop in findOpenIP()
	StartingIP       int      //base ip for building an ip in findOpenIP(). will default to :3410
	IsListening      bool     //has this server registered rpc and listening on http?
	NumTasksAssigned int      //number of map assignments that have been handed out
	MapFileLocations []string //locations of the files created by a mapper
	Output           string   //output
	Table            string   //table name
	ReduceCount      int      //number of reduce tasks to do
	MapDoneCount     int      //map tasks done
	Finished         int
	DoneChannel      chan int
	LogLevel         int
	FilesDownloaded  int
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

func (elt *MasterServer) Ping(sender string, reply *PingResponse) error {
	log.Println("Ping from : ", sender)
	reply.Responded = true
	reply.ResponderAddress = elt.GetServerAddress()
	return nil
}
func (elt *MasterServer) GetWork(_ Request, response *Response) error {
	response.Output = elt.Output
	if len(elt.Tasks) > 0 { // MAP
		log.Printf("Map task %d Assigned", elt.NumTasksAssigned+1)
		response.Type = TASK_MAP
		task := elt.Tasks[0]
		task.Table = elt.Table
		task.NumMapTasks = elt.NumMapTasks

		task.NumReducers = elt.NumReduceTasks
		response.Task = task
		response.StartingIP = elt.StartingIP
		elt.NumTasksAssigned++
		elt.Tasks = elt.Tasks[1:]
	} else if elt.ReduceCount < elt.NumReduceTasks { // REDUCE
		if elt.MapDoneCount >= elt.NumMapTasks {
			log.Printf("Reduce task %d Assigned", elt.ReduceCount+1)
			response.Type = TASK_REDUCE
			var task Task
			task.NumMapTasks = elt.NumMapTasks
			task.NumReducers = elt.NumReduceTasks
			task.WorkerID = elt.ReduceCount
			task.MapFileLocations = elt.MapFileLocations
			elt.ReduceCount++
			response.Task = task
			response.StartingIP = elt.StartingIP
			return nil
		} else {
			response.Type = SLEEP
			return nil
		}
	} else { //Done
		LogF(MESSAGES, "All Jobs Have Been Assigned.")
		response.Type = TASK_DONE
	}

	return nil
}
func StartMaster(config *Config, reduceFunction ReduceFunction) error {
	// Config variables
	//input
	dbName := config.InputFileName
	tableName := config.TableName
	outputName := config.OutputFolderName
	maptasks := config.NumMapTasks
	reduceTasks := config.NumReduceTasks

	LogF(VARS_DEBUG, "Opening %s:%s", dbName, tableName)

	// Load the input data
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		LogF(ERRO_DEBUG, "Error in opening DB \n%v", err)
		return err
	}
	defer db.Close()

	// Count the work to be done
	query, err := db.Query(fmt.Sprintf("select count(*) from %s;", tableName))
	if err != nil {
		LogF(ERRO_DEBUG, "Failed in query sql count\n%v", err)
		return err
	}
	defer query.Close()

	// Split up the data per m
	var count int
	var rowsToWork int
	query.Next()
	query.Scan(&count)
	rowsToWork = int(math.Ceil(float64(count) / float64(maptasks)))
	var tasks []Task
	for i := 0; i < maptasks; i++ {
		var task Task
		task.Type = TASK_MAP
		task.Filename = dbName
		task.Offset = i * rowsToWork
		task.Size = rowsToWork
		task.WorkerID = i
		tasks = append(tasks, task)
	}
	LogF(VARS_DEBUG, "%d Tasks to work", count)
	// Set up the RPC server to listen for workers

	elt := NewMasterServer(*config, &tasks)

	<-elt.DoneChannel

	err = Merge(reduceTasks, reduceFunction, outputName)
	if err != nil {
		LogF(ERRO_DEBUG, "Error from merging\n%v", err)
	}
	/*
		temp := "tmp/"
		output := fmt.Sprintf("%s/", outputName)
		if runtime.GOOS == "windows" {
			temp = "tmp\\"
			output = fmt.Sprintf("%s\\", outputName)
		}
		//log.Println(os.RemoveAll(temp))
		//log.Println(os.RemoveAll(output))
	*/
	return nil
}
func (elt *MasterServer) Notify(request Request, response *Response) error {
	if request.Type == TASK_MAP {
		LogF(MESSAGES, "%d of %d Map Tasks Complete", elt.MapDoneCount+1, elt.NumMapTasks)
		elt.MapFileLocations = append(elt.MapFileLocations, request.Address)
		elt.MapDoneCount++
	} else if request.Type == TASK_REDUCE {
		LogF(MESSAGES, "%d of %d Reduce Tasks Complete", elt.Finished+1, elt.NumReduceTasks)
		elt.Finished++
		LogF(VARS_DEBUG, "Getting reducer#%d output file for master", request.Task.WorkerID)
		time.Sleep(time.Second * 2)
		red_file := fmt.Sprintf("http://%s/reduce_out_%d.sql", request.Address, request.Task.WorkerID)

		res, err := http.Get(red_file)
		if err != nil {
			LogF(SPECIAL_CASE, "Failed getting file from reducer")
			log.Fatal(err)
		}

		file, err := ioutil.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			LogF(SPECIAL_CASE, "Failed to read all using ioutil")
			log.Fatal(err)
		}
		pathName := fmt.Sprintf("%s/", elt.Output)
		if runtime.GOOS == "windows" {
			pathName = fmt.Sprintf("%s\\", elt.Output)
		}
		os.MkdirAll(pathName, 0777)
		filename := fmt.Sprintf("%sreduce_out_%d.sql", pathName, request.Task.WorkerID)
		err = ioutil.WriteFile(filename, file, 0777)
		if err != nil {
			LogF(SPECIAL_CASE, "Couldn't write file \n%s", filename)
			log.Fatal(err)
		}
		if elt.Finished >= elt.NumReduceTasks {
			response.Type = STANDBY
		}
	} else if request.Type == STANDBY {
		if elt.Finished >= elt.NumReduceTasks {
			response.Type = TASK_DONE
			go func() {
				time.Sleep(time.Second * 11)
				elt.DoneChannel <- 1
			}()
		}
	}

	return nil
}

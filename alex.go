package main

import (
	"database/sql"
	"fmt"
	"github.com/mattn/go-sqlite3"
	"log"
	//"bufio"
	//"time"
	//"strings"
	//"os"
	"strconv"
)

//these are used in task type
const (
	WORK_DONE   = "1"
	WORK_MAP    = "2"
	WORK_REDUCE = "3"
	WAIT        = "4"
)

type Task struct {
	WorkerID     int
	Type         int
	Filename     string
	Offset       int
	Size         int
	M            int
	R            int
	Table        string
	MapAddresses []string
}
type Server struct {
	NumMappers  int
	NumReducers int
	Tasks       []Task
}

func (elt *Server) create() error {
	rpc.Register(elt)
	rpc.HandleHTTP()
	listening := false
	nextAddress := 0
	var l net.Listener
	for !listening {
		nextAddress += 1
		listening = true
		listener, err := net.Listen("tcp", n.address)
		if err != nil {
			if nextAddress >= 5 {
				log.Fatal("Quorum is full")
			}
			listening = false
			n.address = n.q[nextAddress]
			log.Println("Address is:", n.address)
		}
		l = listener
	}
	go http.Serve(l, nil)
	return nil
}

//This will be our struct to hold the data supplied by russ
type Pair struct {
	Key   string
	Value string
}

//This can be used to print data to the sql format from any struct
//implement a Print() method for a struct, then it can be converted to SQL
type SQLCommand interface {
	InsertSQL(*sql.DB) error
	QuerySQLFromStructKey(*sql.DB) string
}

func QueryID(ID int, database *sql.DB) string {
	SID := strconv.Itoa(ID)
	return SID
}
func QueryKey(Key string, database *sql.DB) string {
	return "not implemented"
}
func (elt Pair) QuerySQLFromStructKey(database *sql.DB) (ReturnValue string) {
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
func (elt Pair) InsertSQL(database *sql.DB) error {
	_, err := database.Exec("INSERT INTO Pairs (ID, key, value) VALUES (?, ?, ?)", nil, elt.Key, elt.Value)
	return err
}

//in mapreduce, we want to not do db.Begin() just do db.exec()
func DatabaseMutate(database *sql.DB, command SQLCommand) {
	//open a session with the database
	tx, err := database.Begin()
	if err != nil {
		log.Fatalln("Could not begin connection in rundb\n", err)
	}

	err2 := command.InsertSQL(database)

	if err2 != nil {
		log.Fatal(err2)
	}
	//write the changes to the database.
	tx.Commit()
}
func main() {

	var DBDRIVER string

	sql.Register(DBDRIVER, &sqlite3.SQLiteDriver{})

	database, err := sql.Open(DBDRIVER, "mapreducedb")
	if err != nil {
		fmt.Println("Didn't open!")
	}
	if err2 := database.Ping(); err2 != nil {
		fmt.Println("Failed to open connection. Database may not exist")
	}
	var alex Pair
	alex.Key = "Alex's Class"
	alex.Value = "3005"
	//(database)
	DatabaseMutate(database, alex)
	fmt.Println(alex.QuerySQLFromStructKey(database))
}

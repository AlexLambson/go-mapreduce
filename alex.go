package main

import (
	"./akmapreducelib"
	"database/sql"
	//"fmt"
	//"github.com/mattn/go-sqlite3"
	"log"
	//"bufio"
	//"time"
	//"strings"
	//"os"
	"strconv"
)

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

/*func main() {

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
}*/
func main() {
	//cmdLineArgs := os.Args
	//Server comes from mapreduce lib
	var LocalServer MapReduce.Server
	LocalServer = MapReduce.NewServer(0)
	MapReduce.LogF(LocalServer.GetLocalAddress())
	/*
		NumMappers  int
		NumReducers int
		Tasks       []Task
		Address     string
		MaxServers  int
		//base ip for building an ip in getLocalAddress
		//will default to :3410
		StartingIP int
	*/
}

package main

import (
	"database/sql"
	"fmt"
	"github.com/mattn/go-sqlite3"
	//"log"
	//"bufio"
	//"time"
	//"strings"
	//"os"
)

func main() {

	var DBDRIVER string

	sql.Register(DBDRIVER, &sqlite3.SQLiteDriver{})

	fmt.Println(sql.DB.Ping())
}

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
)

type Person struct {
	key       sql.NullInt64
	firstName sql.NullString
	lastName  sql.NullString
	address   sql.NullString
	city      sql.NullString
}

type employee struct {
	empID       sql.NullInt64
	empName     sql.NullString
	empAge      sql.NullInt64
	empPersonId sql.NullInt64
}

func rundb(database *sql.DB) {
	tx, err := database.Begin()
	if err != nil {
		log.Fatalln("Could not begin connection in rundb\n", err)
	}
	result, err2 := database.Exec(
		"CREATE TABLE IF NOT EXISTS Persons ( id integer PRIMARY KEY, LastName varchar(255) NOT NULL, FirstName varchar(255), Address varchar(255), City varchar(255), CONSTRAINT uc_PersonID UNIQUE (id,LastName))")

	if err2 != nil {
		log.Fatal(err2)
	}
	result, err2 = database.Exec(
		"create table IF NOT EXISTS employee (employeeID integer PRIMARY KEY,name varchar(255) NOT null,age int, person_id int, FOREIGN KEY (person_id) REFERENCES persons(id), CONSTRAINT uc_empID UNIQUE (employeeID, person_id, name))")

	if err2 != nil {
		log.Fatal(err2)
	}
	fmt.Println(result.LastInsertId())
	result, err2 = database.Exec(
		"INSERT INTO employee (employeeID, name, age, person_id) VALUES (?, ?, ?, ?)", nil,
		"Swati Soni",
		24, 1)

	if err2 != nil {
		log.Fatal(err2)
	}
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
	rundb(database)
}

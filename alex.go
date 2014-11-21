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

//This can be used to print data to the sql format from any struct
//implement a Print() method for a struct, then it can be converted to SQL
type StructToSQL interface {
	PrintToSQL() string
}

func (elt employee) PrintToSQL() string {
	return fmt.Sprintf("Test employee")
}
func (elt Person) PrintToSQL() string {
	return fmt.Sprintf("Test person")
}

//any struct can be passed to this if it has PrintToSQL() implemented
func test(elt StructToSQL) {
	log.Println(elt.PrintToSQL())
}

//in mapreduce, we want to not do db.Begin() just do db.exec()
func rundb(database *sql.DB) {
	//open a session with the database
	tx, err := database.Begin()
	if err != nil {
		log.Fatalln("Could not begin connection in rundb\n", err)
	}
	//execute a write. creating a table
	result, err2 := database.Exec(
		"CREATE TABLE IF NOT EXISTS Persons ( id integer PRIMARY KEY, LastName varchar(255) NOT NULL, FirstName varchar(255), Address varchar(255), City varchar(255), CONSTRAINT uc_PersonID UNIQUE (id,LastName))")

	if err2 != nil {
		log.Fatal(err2)
	}
	//table creation. Just keep reusing the same results var
	result, err2 = database.Exec(
		"create table IF NOT EXISTS employee (employeeID integer PRIMARY KEY,name varchar(255) NOT null,age int, person_id int, FOREIGN KEY (person_id) REFERENCES persons(id), CONSTRAINT uc_empID UNIQUE (employeeID, person_id, name))")

	if err2 != nil {
		log.Fatal(err2)
	}
	fmt.Println(result.LastInsertId())
	//use "?" for formatting sqlite strings
	result, err2 = database.Exec(
		"INSERT INTO employee (employeeID, name, age, person_id) VALUES (?, ?, ?, ?)", nil,
		"Swati Soni",
		24, 1)

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
	//rundb(database)
	var tim Person
	var timmy employee
	test(tim)
	test(timmy)
}

package main

import (
	"bufio"
	"github.com/alexlambson/mapreduce"
	"log"
	"os"
	"strings"
)

func main() {
	isMaster := false
	Settings := mapreduce.ParseFlagsToSettings()

	if isMaster {
		//var LocalServer mapreduce.MasterServer
		//LocalServer = mapreduce.NewMasterServer(Settings, &Tasks)
		//log.Println(LocalServer.GetServerAddress())
	}
	for {
		reader := bufio.NewReader(os.Stdin)
		line, err := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if err != nil {
			log.Fatal("Can't read string!", err)
		}
	}
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

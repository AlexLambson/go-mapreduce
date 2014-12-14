package main

import (
	"github.com/alexlambson/mapreduce"
	"log"
	"strconv"
	"strings"
	"unicode"
)

func wordCountMapper(key, value string, output chan<- mapreduce.Pair) error {
	defer close(output)
	lst := strings.Fields(value)
	for _, elt := range lst {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, elt)
		if len(word) > 0 {
			output <- mapreduce.Pair{Key: word, Value: "1"}
		}
	}

	return nil
}
func wordCountReducer(key string, values <-chan string, output chan<- mapreduce.Pair) error {
	defer close(output)
	count := 0
	for v, ok := <-values; ok; v, ok = <-values {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := mapreduce.Pair{Key: key, Value: strconv.Itoa(count)}
	output <- p

	return nil
}
func main() {
	isMaster := false
	Settings := mapreduce.ParseFlagsToSettings()
	masterIPString, _ := mapreduce.PortIntToAddressString(Settings.StartingIP)
	if mapreduce.FindOpenIP(Settings.StartingIP) == masterIPString {
		isMaster = true
	}

	if isMaster {
		mapreduce.LogF(mapreduce.MESSAGES, "I am the master on %d", Settings.StartingIP)
		err := mapreduce.StartMaster(&Settings, wordCountReducer)
		if err != nil {
			mapreduce.PrintError(err)
		}
		//LocalServer = mapreduce.NewMasterServer(Settings, &Tasks)
		//log.Println(LocalServer.GetServerAddress())
		log.Println("Task complete!")
	} else {

		log.Println("I am a worker")

		err := mapreduce.StartWorker(wordCountMapper, wordCountReducer, masterIPString)
		if err != nil {
			log.Println(err)
		}

	}
}

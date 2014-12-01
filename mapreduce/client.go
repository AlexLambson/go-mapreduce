package main

import (
	"./mapreduce"
	"flag"

	"log"
	"net"
	"strconv"
	"strings"
	"unicode"
)

func logf(format string, args ...interface{}) {
	if true {
		log.Printf(format, args...)
	}
}

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
	var input, master, output, table string
	var m, r int
	var ismaster, verb bool
	flag.BoolVar(&ismaster, "ismaster", false, "<true> for master, <false> for worker.")
	flag.StringVar(&master, "master", "localhost:3410", "<ip:port> of the master.")
	flag.StringVar(&input, "inputdata", "input.sql", "<db.sql> file to run the task on.")
	flag.StringVar(&table, "table", "pairs", "The name of the table in the database.")
	flag.StringVar(&output, "output", "output", "directory to save the output.")
	flag.IntVar(&m, "m", 1, "The number of map tasks to use.")
	flag.IntVar(&r, "r", 1, "The number of reduce tasks to use.")
	flag.BoolVar(&verb, "v", false, "Verbose switch.")
	flag.Parse()

	if master == "localhost:3410" {
		ismaster = true
	}
	if ismaster {
		master = net.JoinHostPort(mapreduce.GetLocalAddress(), "3410")
		log.Printf("\n\nMap Reduce - Running in Master Mode\n\nFile: %s\nMapers: %d\nReducers: %d\nMaster IP Address: %s\nInput Database: %s\nOutput Directory: %s", input, m, r, master, input, output)
		var config mapreduce.Config
		config.Master = master
		config.InputData = input
		config.Output = output
		config.M = m
		config.R = r
		config.Table = table
		err := mapreduce.StartMaster(&config, wordCountReducer, verb)
		if err != nil {
			log.Println(err)
		}
		log.Println("Task Completed")
	} else {
		log.Printf("\n\nMap Reduce - Running in Worker Mode\nMaster IP: %s", master)

		err := mapreduce.StartWorker(wordCountMapper, wordCountReducer, master, verb)
		if err != nil {
			log.Println(err)
		}
	}
}
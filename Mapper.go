package mapreduce

import (
	"crypto/sha1"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

func hash(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}
func call(address, method string, request Request, response *Response) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		PrintError(err)
		return err
	}
	defer client.Close()

	err = client.Call("MasterServer."+method, request, response)
	if err != nil {
		PrintError(err)
		return err
	}

	return nil
}
func StartWorker(mapFunc MapFunction, reduceFunc ReduceFunction, master string) error {
	errfile := os.Mkdir("\\tmp\\AK47", 1777)
	log.Println(errfile)
	tasks_run := 0
	call_fail := 0
	for {

		/*
		 * Call master, asking for task
		 */

		var resp Response
		var req Request
		LogF(MESSAGES, "Calling master for task")
		LogF(MESSAGES, "Master is: %s", master)
		err := call(master, "GetWork", req, &resp)
		if err != nil {
			LogF(ERRO_DEBUG, "GetWork")
			call_fail++
			tasks_run++
			if call_fail > 5 {
				LogF(ERRO_DEBUG, "Master connection lost :: shutting down")
				break
			}
			continue
		}

		for resp.Type == SLEEP {
			time.Sleep(1e9)
			err = call(master, "GetWork", req, &resp)
			if err != nil {
				LogF(ERRO_DEBUG, "Failed in get work\n%v", err)
				call_fail++
				tasks_run++
				if call_fail > 5 {
					LogF(ERRO_DEBUG, "Master connection lost :: shutting down")
					break
				}
				continue
			}

		}
		task := resp.Task
		var my_address string

		// Walks through the assigned sql records
		// Call the given mapper function
		// Receive from the output channel in a go routine
		// Feed them to the reducer through its own sql files
		// Close the sql files

		if resp.Type == TASK_MAP {
			LogF(MESSAGES, "::::::::::::: NEW MAP TASK :::::::::::::")
			LogF(MESSAGES, "MTask Number: %d", task.WorkerID+1)
			LogF(MESSAGES, "   SQL Range: %d-%d", task.Offset, task.Offset+task.Size)
			// Load data

			db, err := sql.Open("sqlite3", task.Filename)
			if err != nil {
				LogF(ERRO_DEBUG, "sql.Open \n%v", err)
				return err
			}
			defer db.Close()

			LogF(FULL_DEBUG, "Opening %s", task.Filename)

			// Query
			rows, err := db.Query(fmt.Sprintf("select key, value from %s limit %d offset %d;", task.Table, task.Size, task.Offset))
			if err != nil {
				LogF(ERRO_DEBUG, "sql.Query 1 \n%v", err)
				return err
			}
			defer rows.Close()

			LogF(FULL_DEBUG, "Starting Map Function Loop")

			for rows.Next() {
				var key string
				var value string
				rows.Scan(&key, &value)

				// Temp storage
				// Each time the map function emits a key/value pair, you should figure out which reduce task that pair will go to.
				reducer := big.NewInt(0)
				reducer.Mod(hash(key), big.NewInt(int64(task.NumReducers)))

				db_tmp, err := sql.Open("sqlite3", fmt.Sprintf("/tmp/AK47/map_%d_out_%d.sql", task.WorkerID, reducer.Int64()))
				if err != nil {
					LogF(ERRO_DEBUG, "sql.Open - /tmp/map_output/%d/map_out_%d.sql", task.WorkerID, reducer.Int64())
					return err
				}

				// Prepare tmp database
				sqls := []string{
					"create table if not exists data (key text not null, value text not null)",
					"create index if not exists data_key on data (key asc, value asc);",
					"pragma synchronous = off;",
					"pragma journal_mode = off;",
				}
				for _, sql := range sqls {
					_, err = db_tmp.Exec(sql)
					if err != nil {
						LogF(ERRO_DEBUG, "%q: %s\n", err, sql)
						return err
					}
				}

				//type MapFunc func(key, value string, output chan<- Pair) error
				outChan := make(chan Pair)
				go func() {

					err = mapFunc(key, value, outChan)
					if err != nil {
						LogF(ERRO_DEBUG, "MapFunc Error: %q", err)
					}
				}()

				// Get the output from the map function's output channel
				pair := <-outChan
				for pair.Key != "" {
					key, value = pair.Key, pair.Value
					// Write the data locally
					sql := fmt.Sprintf("insert into data values ('%s', '%s');", key, value)
					_, err = db_tmp.Exec(sql)
					if err != nil {
						LogF(ERRO_DEBUG, "sql.exec4\nmap_%d_out_%d.sql\n", task.WorkerID, reducer.Int64())
						LogF(ERRO_DEBUG, "%q: %s\n", err, sql)
						return err
					}
					//log.Println(key, value)
					pair = <-outChan
				}
				db_tmp.Close()
			}

			my_address = FindOpenIP(resp.StartingIP + task.WorkerID)
			// Serve the files so each reducer can get them
			go func(address string) {
				LogF(FULL_DEBUG, "Starting file server for mapout files")

				fileServer := http.FileServer(http.Dir("/tmp/AK47/"))
				LogF(FULL_DEBUG, "Map Job File Server Listening on "+address)
				log.Fatal(http.ListenAndServe(address, fileServer))
			}(my_address)
			LogF(MESSAGES, "Map Job Done - Notifying Master")
		} else if resp.Type == TASK_REDUCE {
			LogF(MESSAGES, "::::::::::::: NEW REDUCE TASK :::::::::::")
			LogF(MESSAGES, "RTask Number: %d", task.WorkerID+1)
			//type ReduceFunc func(key string, values <-chan string, output chan<- Pair) error
			// Load each input file one at a time (copied from each map task)
			var filenames []string
			for i, mapper := range task.MapFileLocations {

				LogF(MESSAGES, "Mapper %d Output file --> Reducer %d", task.WorkerID, i)

				map_file := fmt.Sprintf("http://%s/map_%d_out_%d.sql", mapper, i, task.WorkerID)

				res, err := http.Get(map_file)
				if err != nil {
					LogF(ERRO_DEBUG, "http.Get Failure")
					log.Fatal(err)
				}
				req.Type = DOWNLOAD
				req.Address = my_address
				err = call(master, "Notify", req, &resp)
				if err != nil {
					LogF(ERRO_DEBUG, "FileDL Notify")
					call_fail++
				}

				file, err := ioutil.ReadAll(res.Body)
				res.Body.Close()
				if err != nil {
					LogF(ERRO_DEBUG, "ioutil.ReadAll")
					log.Fatal(err)
				}

				filename := fmt.Sprintf("/tmp/AK47/map_out_%d_mapper_%d.sql", task.WorkerID, i)
				filenames = append(filenames, filename)

				err = ioutil.WriteFile(filename, file, 0777)
				if err != nil {
					LogF(ERRO_DEBUG, "file.Write")
					PrintError(err)
				}
			}

			// Combine all the rows into a single input file
			sqls := []string{
				"create table if not exists data (key text not null, value text not null)",
				"create index if not exists data_key on data (key asc, value asc);",
				"pragma synchronous = off;",
				"pragma journal_mode = off;",
			}
			LogF(FULL_DEBUG, "Aggregating mapper %d's input files", task.WorkerID)

			for _, file := range filenames {
				LogF(FULL_DEBUG, "Current file :: %s", file)

				db, err := sql.Open("sqlite3", file)
				if err != nil {
					PrintError(err)
					continue
				}
				defer db.Close()

				rows, err := db.Query("select key, value from data;")
				if err != nil {
					PrintError(err)
					continue
				}
				defer rows.Close()

				for rows.Next() {
					var key string
					var value string
					rows.Scan(&key, &value)
					sqls = append(sqls, fmt.Sprintf("insert into data values ('%s', '%s');", key, value))
				}
			}

			LogF(FULL_DEBUG, "Aggregating Complete :: Preping Reduce DB")

			reduce_db, err := sql.Open("sqlite3", fmt.Sprintf("/tmp/AK47/reduce_aggregate_%d.sql", task.WorkerID))
			for _, sql := range sqls {
				_, err = reduce_db.Exec(sql)
				if err != nil {
					fmt.Printf("%q: %s\n", err, sql)
				}
			}
			reduce_db.Close()

			reduce_db, err = sql.Open("sqlite3", fmt.Sprintf("/tmp/AK47/reduce_agg_%d.sql", task.WorkerID))
			defer reduce_db.Close()
			rows, err := reduce_db.Query("select key, value from data order by key asc;")
			if err != nil {
				PrintError(err)
				LogF(ERRO_DEBUG, "Query 2")
				return err
			}
			defer rows.Close()

			var key string
			var value string
			rows.Next()
			rows.Scan(&key, &value)

			LogF(FULL_DEBUG, "Starting Reduce Function Loop")

			//type ReduceFunc func(key string, values <-chan string, output chan<- Pair) error
			inChan := make(chan string)
			outChan := make(chan Pair)
			go func() {

				err = reduceFunc(key, inChan, outChan)
				if err != nil {
					LogF(ERRO_DEBUG, "Reduce Func Error")
					PrintError(err)
				}
			}()
			inChan <- value
			current := key

			var outputPairs []Pair
			// Walk through the file's rows, performing the reduce func
			for rows.Next() {
				rows.Scan(&key, &value)
				if key == current {
					inChan <- value
				} else {
					close(inChan)
					p := <-outChan
					outputPairs = append(outputPairs, p)

					inChan = make(chan string)
					outChan = make(chan Pair)
					go func() {
						err = reduceFunc(key, inChan, outChan)
						if err != nil {
							LogF(ERRO_DEBUG, "Reduce Function")
							log.Println(err)
						}
					}()
					inChan <- value
					current = key
				}
			}
			close(inChan)
			p := <-outChan
			outputPairs = append(outputPairs, p)
			db_out, err := sql.Open("sqlite3", fmt.Sprintf("/tmp/AK47/reduce_out_%d.sql", task.WorkerID))
			defer db_out.Close()
			if err != nil {
				PrintError(err)
				LogF(ERRO_DEBUG, "sql.Open - reduce_out_%d.sql", task.WorkerID)
				return err
			}
			sqls = []string{
				"create table if not exists data (key text not null, value text not null)",
				"create index if not exists data_key on data (key asc, value asc);",
				"pragma synchronous = off;", "pragma journal_mode = off;",
			}
			for _, sql := range sqls {
				_, err = db_out.Exec(sql)
				if err != nil {
					LogF(ERRO_DEBUG, "sql.Exec5")
					fmt.Printf("%q: %s\n", err, sql)
					return err
				}
			}

			// Write the data locally

			for _, op := range outputPairs {
				sql := fmt.Sprintf("insert into data values ('%s', '%s');", op.Key, op.Value)
				_, err = db_out.Exec(sql)
				if err != nil {
					LogF(ERRO_DEBUG, "sql.Exec6")
					LogF(ERRO_DEBUG, "%q: %s\n", err, sql)
					return err
				}
			}

			my_address = FindOpenIP(resp.StartingIP + task.WorkerID)
			// Serve the files so each reducer can get them
			// /tmp/reduce_out_%d.sql
			go func(address string) {

				LogF(FULL_DEBUG, "Starting file server for reduceout files")

				fileServer := http.FileServer(http.Dir("/tmp/AK47/"))
				LogF(FULL_DEBUG, "Reduce Job File Server Listening on "+address)
				log.Fatal(http.ListenAndServe(address, fileServer))
			}(my_address)

			LogF(MESSAGES, "Reduce Complete :: Notifying Master")
		} else if resp.Type == TASK_DONE {
			LogF(MESSAGES, "Wait Message    :: 10 seconds")
			time.Sleep(time.Second * 10)
			LogF(MESSAGES, "Cleanup Message :: Removing Temp Files")
			os.RemoveAll("/tmp/AK47")
			break
		} else {
			LogF(ERRO_DEBUG, "INVALID WORK TYPE")
			var err error
			return err
		}

		// Notify the master when I'm done

		req.Task = resp.Task
		req.Type = resp.Type
		req.Address = my_address
		err = call(master, "Notify", req, &resp)
		if err != nil {
			LogF(ERRO_DEBUG, "Notify")
			call_fail++
			tasks_run++
			continue
		}

		if resp.Type == SLEEP {
			for {
				LogF(MESSAGES, "Wait Message    :: 10 seconds")
				time.Sleep(time.Second * 10)

				req.Type = SLEEP
				err = call(master, "Notify", req, &resp)
				if err != nil {
					LogF(ERRO_DEBUG, "Notify Waiting")
					call_fail++
					tasks_run++
				}
				if resp.Type == TASK_DONE {
					break
				}
			}

			LogF(MESSAGES, "Cleanup Message :: Removing Temp Files")
			err = os.RemoveAll("/tmp/AK47")
			if err != nil {
				fmt.Printf("%q", err)
			}
			return nil
		}
		tasks_run++

		if call_fail > 5 {
			LogF(MESSAGES, "Cleanup Message :: Removing Temp Files")
			LogF(MESSAGES, "Master Down     :: Shutting down...")

			break
		}

	}

	return nil
}

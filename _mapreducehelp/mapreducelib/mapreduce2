package mapreduce

import (
	"bufio"
	"crypto/sha1"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"log"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	WORK_DONE   = "1"
	WORK_MAP    = "2"
	WORK_REDUCE = "3"
	WAIT        = "4"
)

const (
	TYPE_MAP = iota
	TYPE_REDUCE
	TYPE_DONE
	TYPE_WAIT
	TYPE_DL
)

type Pair struct {
	Key   string
	Value string
}

type Config struct {
	Master    string
	InputData string
	Output    string
	M         int
	R         int
	Table     string
}

type MapFunc func(key, value string, output chan<- Pair) error
type ReduceFunc func(key string, values <-chan string, output chan<- Pair) error

type Request struct {
	Message string
	Address string
	Type    int
	Work    Work
}

type Response struct {
	Message   string
	Database  string
	Offset    int
	Chunksize int
	Type      int
	Mapper    int
	Reducer   int
	Work      Work
	Output    string
}

type Master struct {
	MapAssi      int
	FilesDL      int
	RFilesDL     int
	Address      string
	M            int
	R            int
	Maps         []Work
	MapAddresses []string
	MapDoneCount int
	ReduceCount  int
	WorkDone     int
	DoneChan     chan int
	Merged       bool
	Table        string
	Output       string
	Verb         bool
}

type Work struct {
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

func (self *Master) GetWork(_ Request, response *Response) error {
	response.Output = self.Output
	if len(self.Maps) > 0 { // MAP
		log.Printf("Map Job %d Assigned", self.MapAssi+1)
		response.Type = TYPE_MAP
		work := self.Maps[0]
		work.Table = self.Table
		work.M = self.M

		work.R = self.R
		response.Work = work
		self.MapAssi++
		self.Maps = self.Maps[1:]
	} else if self.ReduceCount < self.R { // REDUCE
		if self.MapDoneCount >= self.M {
			log.Printf("Reduce Job %d Assigned", self.ReduceCount+1)
			response.Type = TYPE_REDUCE
			var work Work
			work.M = self.M
			work.R = self.R
			work.WorkerID = self.ReduceCount
			work.MapAddresses = self.MapAddresses
			self.ReduceCount = self.ReduceCount + 1
			response.Work = work
			return nil
		} else {
			response.Type = TYPE_WAIT
			return nil
		}
	} else { //Done
		logf("All Jobs Have Been Assigned.")
		response.Type = TYPE_DONE
	}

	return nil
}

func (self *Master) Notify(request Request, response *Response) error {
	if request.Type == TYPE_MAP {
		logf("%d of %d Map Tasks Complete", self.MapDoneCount+1, self.M)
		self.MapAddresses = append(self.MapAddresses, request.Address)
		self.MapDoneCount++
	} else if request.Type == TYPE_REDUCE {
		logf("%d of %d Reduce Tasks Complete", self.WorkDone+1, self.R)
		self.WorkDone++
		if self.Verb {
			logf("Getting reducer#%d output file for master", request.Work.WorkerID)
		}
		time.Sleep(time.Second * 2)
		red_file := fmt.Sprintf("http://%s/reduce_out_%d.sql", request.Address, request.Work.WorkerID)

		res, err := http.Get(red_file)
		if err != nil {
			failure("http.Get")
			log.Fatal(err)
		}

		file, err := ioutil.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			failure("ioutil.ReadAll")
			log.Fatal(err)
		}

		filename := fmt.Sprintf("%s/reduce_out_%d.sql", self.Output, request.Work.WorkerID)

		err = ioutil.WriteFile(filename, file, 0777)
		if err != nil {
			failure("file.Write")
			log.Fatal(err)
		}
		if self.WorkDone >= self.R {
			response.Message = WAIT

		}
	} else if request.Type == TYPE_DL {
		self.FilesDL++
	} else if request.Type == TYPE_WAIT {
		if self.FilesDL == (self.M * self.R) {
			response.Message = WORK_DONE
			go func() {
				time.Sleep(time.Second * 11)
				self.DoneChan <- 1
			}()
		}
	}

	return nil
}

func Merge(R int, reduceFunc ReduceFunc, output string, verb bool) error {
	os.Mkdir("/tmp/tjones", 1777)
	// Combine all the rows into a single input file
	sqls := []string{
		"create table if not exists data (key text not null, value text not null)",
		"create index if not exists data_key on data (key asc, value asc);",
		"pragma synchronous = off;",
		"pragma journal_mode = off;",
	}
	for i := 0; i < R; i++ {
		if verb {
			logf("Aggregating Reducer Output Files")
		}
		db, err := sql.Open("sqlite3", fmt.Sprintf("%s/reduce_out_%d.sql", output, i))
		if err != nil {
			log.Println(err)
			continue
		}
		defer db.Close()

		rows, err := db.Query("select key, value from data;")
		if err != nil {
			fmt.Println(err)
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

	agg_db, err := sql.Open("sqlite3", "/tmp/tjones/aggregate.sql")
	for _, sql := range sqls {
		_, err = agg_db.Exec(sql)
		if err != nil {
			fmt.Printf("%q: %s\n", err, sql)
		}
	}
	agg_db.Close()

	agg_db, err = sql.Open("sqlite3", ("/tmp/tjones/aggregate.sql"))
	defer agg_db.Close()
	rows, err := agg_db.Query("select key, value from data order by key asc;")
	if err != nil {
		log.Println(err)
		failure("sql.Query3")
		return err
	}
	defer rows.Close()

	var key string
	var value string
	rows.Next()
	rows.Scan(&key, &value)

	inChan := make(chan string)
	outChan := make(chan Pair)
	go func() {
		err = reduceFunc(key, inChan, outChan)
		if err != nil {
			failure("reduceFunc")
			log.Println(err)
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
					failure("reduceFunc")
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

	// Prepare tmp database
	db_out, err := sql.Open("sqlite3", fmt.Sprintf("%s/output.sql", output))
	defer db_out.Close()
	if err != nil {
		log.Println(err)
		failure("sql.Open - output.sql")
		return err
	}
	sqls = []string{
		"create table if not exists data (key text not null, value text not null)",
		"create index if not exists data_key on data (key asc, value asc);",
		"pragma synchronous = off;",
		"pragma journal_mode = off;",
	}
	for _, sql := range sqls {
		_, err = db_out.Exec(sql)
		if err != nil {
			failure("sql.Exec1")
			fmt.Printf("%q: %s\n", err, sql)
			return err
		}
	}

	// Write the data locally
	for _, op := range outputPairs {
		sql := fmt.Sprintf("insert into data values ('%s', '%s');", op.Key, op.Value)
		_, err = db_out.Exec(sql)
		if err != nil {
			failure("sql.Exec2")
			fmt.Printf("%q: %s\n", err, sql)
			return err
		}
	}
	return nil
}

func call(address, method string, request Request, response *Response) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		failure("rpc.Dial")
		log.Println(err)
		return err
	}
	defer client.Close()

	err = client.Call("Master."+method, request, response)
	if err != nil {
		failure("rpc.Call")
		log.Println(err)
		return err
	}

	return nil
}

func failure(f string) {
	log.Println("Call", f, "has failed.")
}

func readLine(readline chan string) {
	// Get command
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal("READLINE ERROR:", err)
	}
	readline <- line
}

func logf(format string, args ...interface{}) {
	if true {
		log.Printf(format, args...)
	}
}

func hash(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

func GetLocalAddress() string {
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interfaces")
			}
			for _, a := range addrs {
				ip, _, err := net.ParseCIDR(a.String())
				if err != nil {
					panic("init: failed to parse address for network interface")
				}
				ip4 := ip.To4()
				if ip4 != nil {
					localaddress = ip.String()
					break
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}

	return localaddress
}

/*
 * // MASTER - counts the data
 * select count(*) from data;
 * // Divide the count up among the number of mappers
 */
func StartMaster(config *Config, reduceFunc ReduceFunc, verb bool) error {
	// Config variables
	master := config.Master
	input := config.InputData
	table := config.Table
	output := config.Output
	m := config.M
	r := config.R
	if verb {
		logf("Opening %s:%s", input, table)
	}
	// Load the input data
	db, err := sql.Open("sqlite3", input)
	if err != nil {
		log.Println(err)
		failure("sql.Open")
		return err
	}
	defer db.Close()

	// Count the work to be done
	query, err := db.Query(fmt.Sprintf("select count(*) from %s;", table))
	if err != nil {
		failure("sql.Query4")
		log.Println(err)
		return err
	}
	defer query.Close()

	// Split up the data per m
	var count int
	var chunksize int
	query.Next()
	query.Scan(&count)
	chunksize = int(math.Ceil(float64(count) / float64(m)))
	var works []Work
	for i := 0; i < m; i++ {
		var work Work
		work.Type = TYPE_MAP
		work.Filename = input
		work.Offset = i * chunksize
		work.Size = chunksize
		work.WorkerID = i
		works = append(works, work)
	}
	if verb {
		logf("%d Pairs to process", count)
	}
	// Set up the RPC server to listen for workers
	me := new(Master)
	me.Maps = works
	me.M = m
	me.R = r
	me.ReduceCount = 0
	me.DoneChan = make(chan int)
	me.Table = table
	me.Output = output
	me.Verb = verb

	rpc.Register(me)
	rpc.HandleHTTP()

	go func() {
		err := http.ListenAndServe(master, nil)
		if err != nil {
			failure("http.ListenAndServe")
			log.Println(err.Error())
			os.Exit(1)
		}
	}()

	<-me.DoneChan

	err = Merge(r, reduceFunc, output, verb)
	if err != nil {
		log.Println(err)
	}

	return nil
}

/*
 * // MAPPER - reads its range (limit, offset)
 * select * from data order by key limit 2 offset 1;
 *
 * select distinct key .....
 */
func StartWorker(mapFunc MapFunc, reduceFunc ReduceFunc, master string, verb bool) error {
	os.Mkdir("/tmp/tjones", 1777)
	tasks_run := 0
	call_fail := 0
	for {

		/*
		 * Call master, asking for work
		 */

		var resp Response
		var req Request
		if verb {
			logf("Calling master for work")
		}
		err := call(master, "GetWork", req, &resp)
		if err != nil {
			failure("GetWork")
			call_fail++
			tasks_run++
			if call_fail > 5 {
				logf("Master Not Responding - Shutting Down")
				break
			}
			continue
		}

		for resp.Type == TYPE_WAIT {
			time.Sleep(1e9)
			err = call(master, "GetWork", req, &resp)
			if err != nil {
				failure("GetWork")
				call_fail++
				tasks_run++
				if call_fail > 5 {
					logf("Master Not Responding - Shutting Down")
					break
				}
				continue
			}

		}
		work := resp.Work
		//output := resp.Output
		var myAddress string

		// Walks through the assigned sql records
		// Call the given mapper function
		// Receive from the output channel in a go routine
		// Feed them to the reducer through its own sql files
		// Close the sql files

		if resp.Type == TYPE_MAP {
			logf("*******************Starting New Map Task*******************")
			logf("Map Task Number: %d", work.WorkerID+1)
			log.Printf("Pair Range: %d-%d", work.Offset, work.Offset+work.Size)
			log.Print("Starting Map Job.")
			// Load data
			db, err := sql.Open("sqlite3", work.Filename)
			if err != nil {
				log.Println(err)
				failure("sql.Open")
				return err
			}
			defer db.Close()
			if verb {
				logf("Opening %s", work.Filename)
			}

			// Query
			rows, err := db.Query(fmt.Sprintf("select key, value from %s limit %d offset %d;", work.Table, work.Size, work.Offset))
			if err != nil {
				log.Println(err)
				failure("sql.Query1")
				return err
			}
			defer rows.Close()
			if verb {
				logf("Starting Map Function Loop")
			}
			for rows.Next() {
				var key string
				var value string
				rows.Scan(&key, &value)

				// Temp storage
				// Each time the map function emits a key/value pair, you should figure out which reduce task that pair will go to.
				reducer := big.NewInt(0)
				reducer.Mod(hash(key), big.NewInt(int64(work.R)))

				db_tmp, err := sql.Open("sqlite3", fmt.Sprintf("/tmp/tjones/map_%d_out_%d.sql", work.WorkerID, reducer.Int64()))
				if err != nil {
					log.Println(err)
					failure(fmt.Sprintf("sql.Open - /tmp/map_output/%d/map_out_%d.sql", work.WorkerID, reducer.Int64()))
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
						failure("sql.Exec3")
						fmt.Printf("%q: %s\n", err, sql)
						return err
					}
				}

				//type MapFunc func(key, value string, output chan<- Pair) error
				outChan := make(chan Pair)
				go func() {

					err = mapFunc(key, value, outChan)
					if err != nil {
						failure("mapFunc")
						log.Println(err)
						//return err
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
						failure("sql.Exec4")
						fmt.Printf("map_%d_out_%d.sql\n", work.WorkerID, reducer.Int64())
						fmt.Println(key, value)
						log.Printf("%q: %s\n", err, sql)
						return err
					}
					//log.Println(key, value)
					pair = <-outChan
				}
				db_tmp.Close()
			}

			myAddress = net.JoinHostPort(GetLocalAddress(), fmt.Sprintf("%d", 4000+work.WorkerID))
			// Serve the files so each reducer can get them
			// /tmp/map_output/%d/tmp_map_out_%d.sql
			go func(address string) {
				if verb {
					logf("Starting file server for mapout files")
				}

				fileServer := http.FileServer(http.Dir("/tmp/tjones/"))
				log.Println("Map Job File Server Listening on " + address)
				log.Fatal(http.ListenAndServe(address, fileServer))
			}(myAddress)
			logf("Map Job Done - Notifying Master")
		} else if resp.Type == TYPE_REDUCE {
			logf("*******************Starting New Reduce Task*******************")
			logf("REDUCE ID: %d", work.WorkerID+1)
			//type ReduceFunc func(key string, values <-chan string, output chan<- Pair) error
			// Load each input file one at a time (copied from each map task)
			var filenames []string
			for i, mapper := range work.MapAddresses {
				if verb {
					logf("Getting mapper#%d output file for reducer#%d", work.WorkerID, i)
				}
				map_file := fmt.Sprintf("http://%s/map_%d_out_%d.sql", mapper, i, work.WorkerID)

				res, err := http.Get(map_file)
				if err != nil {
					failure("http.Get")
					log.Fatal(err)
				}
				req.Type = TYPE_DL
				req.Address = myAddress
				err = call(master, "Notify", req, &resp)
				if err != nil {
					failure("FileDL Notify")
					call_fail++
				}

				file, err := ioutil.ReadAll(res.Body)
				res.Body.Close()
				if err != nil {
					failure("ioutil.ReadAll")
					log.Fatal(err)
				}

				filename := fmt.Sprintf("/tmp/tjones/map_out_%d_mapper_%d.sql", work.WorkerID, i)
				filenames = append(filenames, filename)

				err = ioutil.WriteFile(filename, file, 0777)
				if err != nil {
					failure("file.Write")
					log.Fatal(err)
				}
			}

			// Combine all the rows into a single input file
			sqls := []string{
				"create table if not exists data (key text not null, value text not null)",
				"create index if not exists data_key on data (key asc, value asc);",
				"pragma synchronous = off;",
				"pragma journal_mode = off;",
			}
			if verb {
				logf("Aggregating mapper#%d input files.", work.WorkerID)
			}
			for _, file := range filenames {
				if verb {
					logf("Current AggIter file = %s.", file)
				}
				db, err := sql.Open("sqlite3", file)
				if err != nil {
					log.Println(err)
					continue
				}
				defer db.Close()

				rows, err := db.Query("select key, value from data;")
				if err != nil {
					fmt.Println(err)
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
			if verb {
				logf("Aggregating Mapper Files Complete, Preping Reduce DB")
			}
			reduce_db, err := sql.Open("sqlite3", fmt.Sprintf("/tmp/tjones/reduce_aggregate_%d.sql", work.WorkerID))
			for _, sql := range sqls {
				_, err = reduce_db.Exec(sql)
				if err != nil {
					fmt.Printf("%q: %s\n", err, sql)
				}
			}
			reduce_db.Close()

			reduce_db, err = sql.Open("sqlite3", fmt.Sprintf("/tmp/tjones/reduce_aggregate_%d.sql", work.WorkerID))
			defer reduce_db.Close()
			rows, err := reduce_db.Query("select key, value from data order by key asc;")
			if err != nil {
				log.Println(err)
				failure("sql.Query2")
				return err
			}
			defer rows.Close()

			var key string
			var value string
			rows.Next()
			rows.Scan(&key, &value)

			if verb {
				logf("Starting Reduce Function Loop")
			}

			//type ReduceFunc func(key string, values <-chan string, output chan<- Pair) error
			inChan := make(chan string)
			outChan := make(chan Pair)
			go func() {

				err = reduceFunc(key, inChan, outChan)
				if err != nil {
					failure("reduceFunc")
					log.Println(err)
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
							failure("reduceFunc")
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
			db_out, err := sql.Open("sqlite3", fmt.Sprintf("/tmp/tjones/reduce_out_%d.sql", work.WorkerID))
			defer db_out.Close()
			if err != nil {
				log.Println(err)
				failure(fmt.Sprintf("sql.Open - reduce_out_%d.sql", work.WorkerID))
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
					failure("sql.Exec5")
					fmt.Printf("%q: %s\n", err, sql)
					return err
				}
			}

			// Write the data locally

			for _, op := range outputPairs {
				sql := fmt.Sprintf("insert into data values ('%s', '%s');", op.Key, op.Value)
				_, err = db_out.Exec(sql)
				if err != nil {
					failure("sql.Exec6")
					fmt.Printf("%q: %s\n", err, sql)
					return err
				}
			}

			myAddress = net.JoinHostPort(GetLocalAddress(), fmt.Sprintf("%d", 4000+work.WorkerID+work.M))
			// Serve the files so each reducer can get them
			// /tmp/reduce_out_%d.sql
			go func(address string) {
				if verb {
					logf("Starting file server for reduceout files")
				}

				fileServer := http.FileServer(http.Dir("/tmp/tjones/"))
				log.Println("Reduce Job File Server Listening on " + address)
				log.Fatal(http.ListenAndServe(address, fileServer))
			}(myAddress)

			logf("Reduce Job Done - Notifying Master")
		} else if resp.Type == TYPE_DONE {
			logf("Directed To Wait - Sleeping for 10 Seconds.")
			time.Sleep(time.Second * 10)
			logf("Cleanup Command Given - Removing Temp Files.")
			os.RemoveAll("/tmp/tjones")
			break
		} else {
			log.Println("INVALID WORK TYPE")
			var err error
			return err
		}

		/*
		 * Notify the master when I'm done
		 */
		req.Work = resp.Work
		req.Type = resp.Type
		req.Address = myAddress
		err = call(master, "Notify", req, &resp)
		if err != nil {
			failure("Notify")
			call_fail++
			tasks_run++
			continue
		}

		if resp.Message == WAIT {
			for {
				logf("Directed To Wait - Sleeping for 10 Seconds.")
				time.Sleep(time.Second * 10)

				req.Type = TYPE_WAIT
				err = call(master, "Notify", req, &resp)
				if err != nil {
					failure("Notify Waiting")
					call_fail++
					tasks_run++
				}
				if resp.Message == WORK_DONE {
					break
				}
			}

			logf("Cleanup Command Given - Removing Temp Files.")
			err = os.RemoveAll("/tmp/tjones")
			if err != nil {
				fmt.Printf("%q",err)
			}
			return nil
		}
		tasks_run++

		if call_fail > 5 {
			logf("Master Not Responding - Shutting Down")
			break
		}

	}

	return nil
}
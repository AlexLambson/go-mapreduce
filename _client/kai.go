package main

/*
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

type Request struct {
	Message string
	Address string
	Type    int
	Work    Work
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

//// Master

func StartMaster(config *Config, reduceFunc ReduceFunc, verb bool) error {
	// Config variables
	master := config.Master
	input := config.InputData
	table := config.Table
	output := config.Output
	m := config.M
	r := config.R
	// if verb {
	// 	logf("Opening %s:%s", input, table)
	// }
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
	// if verb {
	// 	logf("%d Pairs to process", count)
	// }
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



///// CALL FUNCTION STUFF
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

///// READ LINES
func readLine(readline chan string) {
	// Get command
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal("READLINE ERROR:", err)
	}
	readline <- line
}

//// GET ADDRESS AND HASH FUNCTIONS

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

*/

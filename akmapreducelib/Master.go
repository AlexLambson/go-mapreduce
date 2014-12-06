package MapReduce

//these are used in task type
const (
	WORK_DONE   = "1"
	WORK_MAP    = "2"
	WORK_REDUCE = "3"
	WAIT        = "4"
)

type Config struct {
	MasterIP       string
	InputData      string
	OutputFileName string
	MapperNum      int
	ReducerNum     int
	TableName      string
}
type Task struct {
	//id of worker who is working on it
	WorkerID int
	//type of work, see consts
	Type int
	//file name of sqlite file
	Filename string
	//starting row
	Offset int
	//number of rows to work
	Size int
	//number of mappers
	NumMappers  int
	NumReducers int
	//table name to work on
	Table string
	//evrybody in the quorum
	MapAddresses []string
}
type Server struct {
	NumMappers  int
	NumReducers int
	Tasks       []Task
}

func (elt *Server) create() error {
	rpc.Register(elt)
	rpc.HandleHTTP()
	listening := false
	nextAddress := 0
	var l net.Listener
	for !listening {
		nextAddress += 1
		listening = true
		listener, err := net.Listen("tcp", n.address)
		if err != nil {
			if nextAddress >= 5 {
				log.Fatal("Quorum is full")
			}
			listening = false
			n.address = n.q[nextAddress]
			log.Println("Address is:", n.address)
		}
		l = listener
	}
	go http.Serve(l, nil)
	return nil
}

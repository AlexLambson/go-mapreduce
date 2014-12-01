package main

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
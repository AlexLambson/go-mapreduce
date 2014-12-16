package mapreduce

import (
	"flag"
)

func ParseFlagsToSettings() (Settings Config) {
	var file, output, table string
	var maptasks, reducetasks int
	var logLevel, masterip int
	var isMaster bool
	flag.IntVar(&masterip, "masterip", 3410, "<port int> of the master.")
	flag.StringVar(&file, "file", "input.sqlite3", "<db.sqlite3> file to run the task on.")
	flag.StringVar(&table, "table", "pairs", "The name of the table in the database.")
	flag.StringVar(&output, "outputfile", "output", "directory to save the output.")
	flag.IntVar(&maptasks, "m", 1, "The number of map tasks to use.")
	flag.IntVar(&reducetasks, "r", 1, "The number of reduce tasks to use.")
	flag.IntVar(&logLevel, "log", 0, "<int> Log Level:\n    0 = full\n    1 = variables\n    2 = errors\n    3 = messages\n")
	flag.BoolVar(&isMaster, "master", false, "<true> <false> || am I the master?")
	flag.Parse()

	Settings.InputFileName = file
	Settings.OutputFolderName = output
	Settings.NumMapTasks = maptasks
	Settings.NumReduceTasks = reducetasks
	Settings.TableName = table
	Settings.LogLevel = logLevel
	Settings.StartingIP = masterip
	Settings.IsMaster = isMaster
	return Settings
}

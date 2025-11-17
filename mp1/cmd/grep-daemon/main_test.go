package main

import (
	"bufio"
	"fmt"
	. "g14-mp3/mp1/resources/rpc-api"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestGrepService_Grep(t *testing.T) {
	homeDir, _ := os.UserHomeDir()
	var tests = []struct {
		name      string
		args      GrepArgs
		reply     string
		lineCount int
		VMNumber  string
	}{
		{
			name: "find exactly one line",
			args: GrepArgs{
				VMNumber: "01",
				Args:     []string{"-H", "Illinois"},
			},
			reply:     fmt.Sprintf("%s/vm01.log:LEVEL3: University of Illinois at Urbana - Champaign\n", homeDir),
			lineCount: 1,
			VMNumber:  "01",
		},
		{
			name: "no match found",
			args: GrepArgs{
				VMNumber: "01",
				Args:     []string{"-H", "random word"},
			},
			reply:     "",
			lineCount: 0,
			VMNumber:  "01",
		},

		{
			name: "no args array",
			args: GrepArgs{
				VMNumber: "01",
				Args:     []string{},
			},
			reply:     "",
			lineCount: 0,
			VMNumber:  "01",
		},
		{
			name: "args is nil",
			args: GrepArgs{
				VMNumber: "01",
				Args:     nil,
			},
			reply:     "",
			lineCount: 0,
			VMNumber:  "01",
		},
		{
			name: "Multiple matches",
			args: GrepArgs{
				VMNumber: "01",
				Args:     []string{"Multi-line test number"},
			},
			reply: "LEVEL1: Multi-line test number one\n" +
				"LEVEL5: Multi-line test number two, test should catch both lines\n",
			lineCount: 2,
			VMNumber:  "01",
		},
	}
	var runner GrepService
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			generateLogFile(test.args.VMNumber)
			var reply GrepReply
			err := runner.Grep(&test.args, &reply)
			if err != nil {
				t.Errorf("err should be nil, but got %v", err)
			} else if string(reply.Output) != test.reply {
				t.Errorf("reply should be %s,\n but got %s", test.reply, string(reply.Output))
			} else if reply.VMNumber != test.args.VMNumber {
				t.Errorf("VMNumber should be %s,\n but got %s", test.args.VMNumber, reply.VMNumber)
			} else if reply.LineCount != test.lineCount {
				t.Errorf("LineCount should be %d,\n but got %d", test.lineCount, reply.LineCount)
			}
		})
	}

	// Handle args and reply being nil separately as they are special cases
	t.Run("nil GrepArgs struct", func(t *testing.T) {
		var reply GrepReply
		err := runner.Grep(nil, &reply)
		if err == nil {
			t.Errorf("expected an error for nil args, but got nil")
		} else if err.Error() != "GrepArgs must not be nil" {
			t.Errorf("err should be %v, but got %v", "GrepArgs must not be nil", err)
		}
	})

	t.Run("nil GrepReply struct", func(t *testing.T) {
		args := GrepArgs{
			VMNumber: "11",
			Args:     []string{"-n", "irrelevant"},
		}
		err := runner.Grep(&args, nil)

		if err == nil {
			t.Errorf("expected an error for nil args, but got nil")
		} else if err.Error() != "GrepReply must not be nil" {
			t.Errorf("err should be %v, but got %v", "GrepArgs must not be nil", err)
		}
	})

	// Cover the non-ExitError path
	t.Run("command not found", func(t *testing.T) {
		t.Setenv("PATH", "") // emulate not finding grep

		args := GrepArgs{
			VMNumber: "01",
			Args:     []string{"-H", "Illinois"},
		}
		var reply GrepReply
		err := runner.Grep(&args, &reply)

		if err == nil {
			t.Errorf("expected an error because command should not be found, but got nil")
		}

		if reply.Output != nil {
			t.Errorf("reply.Output should be nil on this error, but it was not")
		}
	})

	t.Run("File not found", func(t *testing.T) {
		var reply GrepReply
		args := GrepArgs{VMNumber: "912839123", Args: []string{"irrelevant"}}
		err := runner.Grep(&args, &reply)
		if err != nil {
			t.Errorf("err should be nil, but got %v", err)
		}
		hd, _ := os.UserHomeDir()
		if string(reply.Output) != fmt.Sprintf("grep: %s/vm912839123.log: No such file or directory\n", hd) {
			t.Errorf("reply.Output should be grep: %s/vm912839123.log: No such file or directory\n, but got %s", hd, string(reply.Output))
		} else if reply.VMNumber != "912839123" {
			t.Errorf("reply.VMNumber should be %s, but got %s", "912839123", reply.VMNumber)
		} else if reply.LineCount != 0 {
			t.Errorf("reply.LineCount should be %d, but got %d", 0, reply.LineCount)
		}
	})

}

func generateLogFile(vmNum string) {
	homeDir, _ := os.UserHomeDir()
	logFile, _ := os.Create(filepath.Join(homeDir, fmt.Sprintf("vm%s.log", vmNum)))

	writer := bufio.NewWriter(logFile)

	defer func() {
		_ = logFile.Close()
		_ = writer.Flush()
	}()

	knownLines := []string{
		"LEVEL1: System outage - fault in distributed system",
		"LEVEL2: THIS is bad :(",
		"LEVEL1: Multi-line test number one",
		"LEVEL3: BIG FAT ERROR, no bueno",
		"LEVEL2: Water bottle testing Indy my goat",
		"LEVEL3: University of Illinois at Urbana - Champaign",
		"LEVEL5: Multi-line test number two, test should catch both lines",
	}

	// Random log components
	prefixes := []string{"LEVEL1", "LEVEL2", "LEVEL3", "LEVEL4", "LEVEL5"}
	messages := []string{
		"this is a testing log line",
		"logs are cool which is why we test",
		"I love logs ",
		"Mihir is so cool",
		"Manan is cooler",
		"Special shoutout to my unit testing homies",
		"yipee woohooo we are the unit testing crew",
		"Grainger CS is the best CS",
		"CS majors are not stinky",
		"ECE versus CS, the greatest battle in history",
	}

	// write known lines
	for _, line := range knownLines {
		_, _ = writer.WriteString(line + "\n")
	}

	// write randomized logs
	for i := 0; i < 20; i++ {
		levelIdx := rand.Intn(len(prefixes))
		messageIdx := rand.Intn(len(messages))
		_, _ = writer.WriteString(fmt.Sprintf("%s: %s\n", prefixes[levelIdx], messages[messageIdx]))
	}
	// write random chars
	for i := 0; i < 10000; i++ {
		_ = writer.WriteByte(byte(rand.Intn(126-33+1) + 33))
		if i%100 == 0 {
			_, _ = writer.WriteString("\n")
		}
	}
}

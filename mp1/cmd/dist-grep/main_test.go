package main

import (
	"errors"
	"fmt"
	. "g14-mp4/mp1/resources/rpc-api"
	"io"
	"net/rpc"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestReadServerAddresses(t *testing.T) {
	tests := []struct {
		name         string
		fileContents []byte
		expected     []string
	}{
		{
			name:         "Successful File Read",
			fileContents: []byte("addr1\n192.168.50.100\nfa25-cs425-1402.cs.illinois.edu\n"),
			expected:     []string{"addr1", "192.168.50.100", "fa25-cs425-1402.cs.illinois.edu", ""},
		},
		{
			name:         "Empty File",
			fileContents: []byte(""),
			expected:     []string{""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			originalContent := makeServersConf(test.fileContents)
			output, err := readServerAddresses()
			if err != nil {
				t.Errorf("Unexpected error while reading server addresses: %v", err)
			}
			if !reflect.DeepEqual(output, test.expected) {
				t.Errorf("\nExpected: %v\nActual: %v", test.expected, output)
			}
			restoreOriginalContent(originalContent)
		})
	}
	wd, _ := os.Getwd()
	filePath := filepath.Join(wd, "servers.conf")
	_ = os.Remove(filePath)

	t.Run("File does not exist", func(t *testing.T) {
		_, err := readServerAddresses()
		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("Expected os.ErrNotExist, but got %v", err)
		}

	})
}

func TestDistributedService(t *testing.T) {
	wd, _ := os.Getwd()
	fp := fmt.Sprintf("%s/../../servers.conf", wd)
	content, _ := os.ReadFile(fp)
	servers := strings.Split(string(content), "\n")
	if servers[len(servers)-1] == "" {
		servers = servers[:len(servers)-1]
	}

	_ = os.WriteFile(filepath.Join(wd, "servers.conf"), content, 0644)

	tests := []struct {
		name         string
		fileContents []string
		osArgs       []string
		expected     []string
	}{
		{
			name: "Only odd Vms have matches",
			fileContents: []string{
				"Match\n",
				"Should not appear\n",
				"Match\n",
				"Should not appear\n",
				"Match\n",
				"Should not appear\n",
				"Match\n",
				"Should not appear\n",
				"Match\n",
				"Should not appear\n",
			},
			osArgs: []string{
				"-H",
				"Match",
			},
			expected: []string{
				"Number of Matches from VM1: 1",
				"/home/manandp2/vm1.log:Match",
				"Number of Matches from VM2: 0",
				"Number of Matches from VM3: 1",
				"/home/manandp2/vm3.log:Match",
				"Number of Matches from VM4: 0",
				"Number of Matches from VM5: 1",
				"/home/manandp2/vm5.log:Match",
				"Number of Matches from VM6: 0",
				"Number of Matches from VM7: 1",
				"/home/manandp2/vm7.log:Match",
				"Number of Matches from VM8: 0",
				"Number of Matches from VM9: 1",
				"/home/manandp2/vm9.log:Match",
				"Number of Matches from VM10: 0",
				"Total number of matching lines summed across all VMs: 5",
				"Results from vm1:",
				"Results from vm2:",
				"Results from vm3:",
				"Results from vm4:",
				"Results from vm5:",
				"Results from vm6:",
				"Results from vm7:",
				"Results from vm8:",
				"Results from vm9:",
				"Results from vm10:",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			for i, addr := range servers {
				server, _ := rpc.Dial("tcp", addr)
				vmNumber := addr[13:15]
				if vmNumber[0] == '0' {
					vmNumber = string(vmNumber[1])
				}
				args := GenerateLogArgs{VMNumber: vmNumber, Data: []byte(test.fileContents[i])}
				err := server.Call("GrepService.GenerateLogs", &args, nil)
				if err != nil {

					t.Errorf("Unexpected error while calling GrepService.GenerateLogs: %v", err)
				}
				_ = server.Close()
			}
			read, write, _ := os.Pipe()
			stdout := os.Stdout
			os.Stdout = write
			osArgs := os.Args
			os.Args = append([]string{""}, test.osArgs...)
			main()
			os.Args = osArgs
			os.Stdout = stdout
			_ = write.Close()
			output, _ := io.ReadAll(read)
			outputArr := strings.Split(string(output), "\n")
			outputArr = outputArr[:len(outputArr)-1]
			slices.Sort(outputArr)
			slices.Sort(test.expected)
			if !reflect.DeepEqual(outputArr, test.expected) {
				t.Errorf("Expected\n %s,\n got\n %s", test.expected, outputArr)
			}
			_ = read.Close()

		})
	}
	_ = os.Remove(filepath.Join(wd, "servers.conf"))
}

func makeServersConf(fileContents []byte) []byte {
	wd, _ := os.Getwd()
	filePath := filepath.Join(wd, "servers.conf")
	originalContent, _ := os.ReadFile(filePath)
	serverFile, _ := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	defer func() {
		_ = serverFile.Close()
	}()
	_ = os.WriteFile(filePath, fileContents, 0644)
	return originalContent
}

func restoreOriginalContent(originalContent []byte) {
	wd, _ := os.Getwd()
	filePath := filepath.Join(wd, "servers.conf")
	file, _ := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	defer func() {
		_ = file.Close()
	}()
	_, _ = file.Write(originalContent)
}

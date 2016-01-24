// Implementation of the Analysis Bots Workers.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	remote_worker "github.com/AnalysisBotsPlatform/platform/worker"
	"github.com/AnalysisBotsPlatform/worker/worker"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

//
// Global state
//

// File system cache
const cache_path_var = "CACHE_PATH"

var cache_path = os.Getenv(cache_path_var)

// Worker service
const worker_host_var = "WORKER_HOST"
const worker_port_var = "WORKER_PORT"

var worker_host = os.Getenv(worker_host_var)
var worker_port = os.Getenv(worker_port_var)

// Persistent configuration
type Config struct {
	Token string
}

var config = &Config{}

//
// Entry point
//

// TODO document this
func main() {
	// check environment
	_, cache := os.LookupEnv(cache_path_var)
	_, whost := os.LookupEnv(worker_host_var)
	_, wport := os.LookupEnv(worker_port_var)
	if !cache || !whost || !wport {
		fmt.Printf("Application settings missing!\n"+
			"Please set the %s, %s and %s environment variables.\n",
			cache_path_var, worker_host_var, worker_port_var)
		return
	}

	cfg_file_path := os.ExpandEnv("${HOME}/.analysisbots-worker.conf")
	if _, err := os.Stat(cfg_file_path); os.IsNotExist(err) {
		fmt.Println("Configuration file does not exist!")
		fmt.Println("Perform initial registration:")
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter registration token: ")
		token, _ := reader.ReadString('\n')
		token = strings.TrimSpace(token)
		fmt.Print("Enter name for worker: ")
		name, _ := reader.ReadString('\n')
		name = strings.TrimSpace(name)
		fmt.Print("Should the worker be shared? [yN]: ")
		shared_in, _ := reader.ReadString('\n')
		shared_in = strings.TrimSpace(shared_in)
		var shared bool
		if shared_in == "" || shared_in == "n" || shared_in == "N" {
			shared = false
		} else if shared_in == "y" || shared_in == "Y" {
			shared = true
		} else {
			fmt.Println("Unexpected input!")
			return
		}
		client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%s", worker_host,
			worker_port))
		if err != nil {
			fmt.Println(err)
			return
		}
		new_worker := remote_worker.NewWorker{
			User_token: token,
			Name:       name,
			Shared:     shared,
		}
		if err := client.Call("WorkerAPI.RegisterNewWorker", new_worker,
			&config.Token); err != nil {
			fmt.Println(err)
			return
		}
		client.Close()
		cfg_file, _ := os.Create(cfg_file_path)
		defer cfg_file.Close()
		encoder := json.NewEncoder(cfg_file)
		if err := encoder.Encode(config); err != nil {
			fmt.Println(err)
			return
		}
	} else {
		cfg_file, _ := os.Open(cfg_file_path)
		defer cfg_file.Close()
		decoder := json.NewDecoder(cfg_file)
		if err := decoder.Decode(config); err != nil {
			fmt.Println(err)
			return
		}
	}

	// connect to worker controller
	client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%s", worker_host,
		worker_port))
	if err != nil {
		fmt.Println(err)
		return
	}

	// initialize background worker
	fmt.Println("Worker start ...")
	if dir, err := filepath.Abs(cache_path); err != nil {
		fmt.Println(err)
		return
	} else {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			fmt.Println("Cache directory does not exist!")
			fmt.Printf("Create cache directory %s\n", dir)
			if err := os.MkdirAll(dir, 0755); err != nil {
				fmt.Println("Cache directory cannot be created!")
				fmt.Println(err)
				return
			}
		}
		worker.Init(dir, client)
	}

	// make sure program terminates correctly
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs

		// unregister and disconnect
		var ack bool
		client.Call("WorkerAPI.UnregisterWorker", config.Token, &ack)
		client.Close()

		fmt.Println("... worker terminated")
		os.Exit(0)
	}()

	// register
	var ack bool
	if err := client.Call("WorkerAPI.RegisterWorker", config.Token,
		&ack); err != nil {
		fmt.Println(err)
		return
	}

	// wait for task and run it
	for {
		var task remote_worker.Task
		if err := client.Call("WorkerAPI.GetTask", config.Token,
			&task); err != nil {
			fmt.Println(err)
			if err != remote_worker.InvalidToken {
				return
			}
			continue
		}

		go worker.RunTask(&task)

		var canceled bool
		if err := client.Call("WorkerAPI.WaitForTaskCancelation", task,
			&canceled); err != nil {
			fmt.Println(err)
		}

		if canceled {
			worker.Cancle()
		}
	}
}

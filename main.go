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

// configuration file location
var cfg_file_path = os.ExpandEnv("${HOME}/.analysisbots-worker.conf")

// Persistent configuration
type Config struct {
	Token string
}

var config = &Config{}

//
// Helper functions
//

// Verifies that no error occurred. If an error occurred it is printed and the
// program terminates.
func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Verifies that all environment variables are set. Fails if not all are set by
// returning an error.
func verifyEnvironment() error {
	_, cache := os.LookupEnv(cache_path_var)
	_, whost := os.LookupEnv(worker_host_var)
	_, wport := os.LookupEnv(worker_port_var)

	if !cache || !whost || !wport {
		return fmt.Errorf("Application settings missing!\n"+
			"Please set the %s, %s and %s environment variables.",
			cache_path_var, worker_host_var, worker_port_var)
	}

	return nil
}

// Ask user to input all necessary information in order to register a new
// worker. The acquired information is returned in form of a NewWorker structure
// only if no error occurred.
func getRegistrationInformation() (*remote_worker.NewWorker, error) {
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
		return nil, fmt.Errorf("Unexpected input!")
	}

	return &remote_worker.NewWorker{
		User_token: token,
		Name:       name,
		Shared:     shared,
	}, nil
}

// Acquire all necessary information to register the Worker and then register
// the Worker at the server.
func registerWorker(cfg_file_path string) error {
	new_worker, err := getRegistrationInformation()
	if err != nil {
		return err
	}

	// connect to Worker server
	client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%s", worker_host,
		worker_port))
	if err != nil {
		return err
	}

	// register new worker (returns a token that identifies the Worker uniquely)
	if err := client.Call("WorkerAPI.RegisterNewWorker", new_worker,
		&config.Token); err != nil {
		return err
	}
	client.Close()

	// create configuration file
	cfg_file, _ := os.Create(cfg_file_path)
	defer cfg_file.Close()

	// write token to configuration file
	encoder := json.NewEncoder(cfg_file)
	if err := encoder.Encode(config); err != nil {
		return err
	}

	return nil
}

// Load the configuration file or if it does not exist start the Worker
// registration procedure.
func loadConfigOrRegister() error {
	// configuration does not exist?
	if _, err := os.Stat(cfg_file_path); os.IsNotExist(err) {
		return registerWorker(cfg_file_path)
	}

	// open configuration file and read it to Config structure
	cfg_file, _ := os.Open(cfg_file_path)
	defer cfg_file.Close()

	decoder := json.NewDecoder(cfg_file)
	if err := decoder.Decode(config); err != nil {
		return err
	}

	return nil
}

//
// Entry point
//

// Fetch and run tasks indefinitely. Start by reading the configuration and if
// it does not exist create it by registering the Worker. Then establish a
// connection to the server and run a task execution loop (one task at a time).
func main() {
	must(verifyEnvironment())
	must(loadConfigOrRegister())

	// connect to Worker controller
	client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%s", worker_host,
		worker_port))
	must(err)

	// initialize Worker
	fmt.Println("Worker start ...")
	cache_directory, err := filepath.Abs(cache_path)
	must(err)

	if _, err := os.Stat(cache_directory); os.IsNotExist(err) {
		fmt.Println("Cache directory does not exist!")
		fmt.Printf("Create cache directory %s\n", cache_directory)
		if err := os.MkdirAll(cache_directory, 0755); err != nil {
			must(fmt.Errorf("Cache directory cannot be created!\n%v", err))
		}
	}
	worker.Init(cache_directory, client)

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
		fmt.Fprintln(os.Stderr, err)
		fmt.Println("Remove configuration file!")
		os.Remove(cfg_file_path)
		fmt.Println("Please restart the worker.")
		sigs <- syscall.SIGINT
	}

	// wait for task and run it
	for {
		var task remote_worker.Task
		if err := client.Call("WorkerAPI.GetTask", config.Token,
			&task); err != nil {
			if err != remote_worker.InvalidToken {
				fmt.Fprintln(os.Stderr, err)
				sigs <- syscall.SIGINT
			}
			continue
		}

		cancel := make(chan bool)
		go worker.RunTask(&task, cancel)

		var canceled bool
		if err := client.Call("WorkerAPI.WaitForTaskCancelation", task,
			&canceled); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}

		if canceled {
			cancel <- true
		}
		close(cancel)
	}
}

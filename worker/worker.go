// Background worker.
package worker

import (
	"fmt"
	"github.com/AnalysisBotsPlatform/platform/utils"
	remote_worker "github.com/AnalysisBotsPlatform/platform/worker"
	"io"
	"io/ioutil"
	"net/rpc"
	"os"
	"os/exec"
	"syscall"
)

// Directory (within the cache directory) where projects are found.
const projects_directory = "projects"

// TODO document this
const patches_directory = "patches"

// Directory where the application can store temporary data on the file system.
var cache_directory string

// Length of the directory names for the cloned GitHub projects.
// NOTE: If the project gets many users, this number should be raised.
const directory_length = 8

// TODO document this
var conn *rpc.Client

// TODO document this
var cancel chan bool

// Initialization of the worker. Sets up the channel store and the cache
// directory is set.
func Init(path_to_cache string, connection *rpc.Client) {
	cache_directory = path_to_cache
	conn = connection
	cancel = make(chan bool, 1)
}

// Cancels the running task specified by the given task id using the channel.
// Also updates the database entry accordingly.
// func Cancle(tid string) error {
// TODO document this
func Cancle() {
	cancel <- true
}

// Checks if the task was canceled.
func checkForCanclation() bool {
	select {
	case <-cancel:
		return true
	default:
		return false
	}
}

// Waits on the channel for a cancelation action. If such an action is received,
// the corresponding process is terminated (Docker container) and the `execBot`
// function is able to continue its execution.
func waitForCanclation(returnChn, abortWait chan bool, cmd *exec.Cmd) {
	select {
	case <-cancel:
		cmd.Process.Kill()
		returnChn <- true
	case <-abortWait:
	}
}

// Executes the task, i.e. runs the Bot as a Docker container and waits for its
// completion. After the tasks execution terminated, the output and exit_code
// output arguments are set appropriatly. In addition a signal is sent on the
// cancelation channel which allows `waitForCancelation` to continue.
func execBot(returnChn chan bool, cmd *exec.Cmd, stdout, stderr io.ReadCloser,
	stdout_out, stderr_out *string, exit_code *int) {
	out, _ := ioutil.ReadAll(stdout)
	err, _ := ioutil.ReadAll(stderr)
	*stdout_out = string(out)
	*stderr_out = string(err)
	if err := cmd.Wait(); err != nil {
		if exiterr, ok := err.(*exec.ExitError); ok {
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				*exit_code = status.ExitStatus()
			}
		}
	} else {
		*exit_code = 0
	}
	defer func() { recover() }()
	returnChn <- true
}

// Cleans up the project cache, i.e. the cloned project is removed from file
// system.
func cleanProjectCache(directory string) {
	rmDirectoryCmd := exec.Command("rm", "-rf", directory)
	if err := rmDirectoryCmd.Run(); err != nil {
		fmt.Println(err)
	}
}

// Preperation steps:
// - Creates the project cache directory if nesessary.
// - Fetches the bot from DockerHub.
// General:
// - Creates a new clone of the repository.
// NOTE: Later this may be changed to a pull instead of clone, i.e. a cloned
// repository is reused.
// - The Bot is executed on the cloned project. This includes the creation of a
// new Docker container from the Bot's Docker image.
// - Waits for completion of the Bot's execution.
// - Cleans up project cache directory (removes clone).
// - Publishes results accordingly (this includes exit status and output)
// TODO documentation
func RunTask(task *remote_worker.Task) {
	var ack bool

	// create project cache directory if necessary
	dir := fmt.Sprintf("%s/%s", cache_directory, projects_directory)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0755); err != nil {
			conn.Call("WorkerAPI.PublishTaskResult", remote_worker.Result{
				Tid:         task.Id,
				Stdout:      "",
				Stderr:      "Cannot create project cache directory!",
				Exit_status: -1,
			}, &ack)
			<-cancel
			return
		}
	}

	if task.Patch {
		dir := fmt.Sprintf("%s/%s/%d", cache_directory, patches_directory,
			task.Id)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err := os.MkdirAll(dir, 0755); err != nil {
				conn.Call("WorkerAPI.PublishTaskResult", remote_worker.Result{
					Tid:         task.Id,
					Stdout:      "",
					Stderr:      "Cannot create patches cache directory!",
					Exit_status: -1,
				}, &ack)
				<-cancel
				return
			}
		}
	}

	// fetch Bot from DockerHub
	dockerPullCmd := exec.Command("docker", "pull", task.Bot)
	if output, err := dockerPullCmd.CombinedOutput(); err != nil {
		// NOTE This should not happen. Either docker is not available or the
		// Bot was removed from the DockerHub. One might want to invalidate the
		// Bot in case err is an ExitError.
		err_msg := fmt.Sprintln(err)
		err_msg = fmt.Sprintf("%s%s", err_msg, output)
		conn.Call("WorkerAPI.PublishTaskResult", remote_worker.Result{
			Tid:         task.Id,
			Stdout:      "",
			Stderr:      err_msg,
			Exit_status: -1,
		}, &ack)
		<-cancel
		return
	}
	if checkForCanclation() {
		return
	}

	// NOTE reuse cloned project
	directory := ""
	path := ""
	exists := true
	for exists {
		path = fmt.Sprintf("%s/%s", projects_directory,
			utils.RandString(directory_length))
		directory = fmt.Sprintf("%s/%s", cache_directory, path)
		if _, err := os.Stat(directory); os.IsNotExist(err) {
			if err := os.Mkdir(directory, 0755); err != nil {
				conn.Call("WorkerAPI.PublishTaskResult", remote_worker.Result{
					Tid:         task.Id,
					Stdout:      "",
					Stderr:      "Cannot create project target directory!",
					Exit_status: -1,
				}, &ack)
				<-cancel
				return
			}
			exists = false
		}
	}
	gitPullCmd := exec.Command("git", "clone",
		fmt.Sprintf("https://%s@github.com/%s.git", task.GH_token,
			task.Project), directory)
	if output, err := gitPullCmd.CombinedOutput(); err != nil {
		err_msg := fmt.Sprintln(err)
		err_msg = fmt.Sprintf("%s%s", err_msg, output)
		conn.Call("WorkerAPI.PublishTaskResult", remote_worker.Result{
			Tid:         task.Id,
			Stdout:      "",
			Stderr:      err_msg,
			Exit_status: -1,
		}, &ack)
		<-cancel
		return
	}
	defer cleanProjectCache(directory)
	if checkForCanclation() {
		return
	}

	// run Bot on Project
	var botCmd *exec.Cmd
	if task.Patch {
		botCmd = exec.Command("docker", "run", "--rm", "--memory=\"128m\"",
			"--cpuset-cpus=\"1\"", "-v", fmt.Sprintf("%s:/%s:ro", directory,
				path), "-v", fmt.Sprintf("%s/%s/%d:/patch", cache_directory,
				patches_directory, task.Id), task.Bot, path, "/patch")
	} else {
		botCmd = exec.Command("docker", "run", "--rm", "--memory=\"128m\"",
			"--cpuset-cpus=\"1\"", "-v", fmt.Sprintf("%s:/%s:ro", directory,
				path), task.Bot, path)
	}
	cancleChn := make(chan bool)
	abortChn := make(chan bool)
	execChn := make(chan bool)
	defer close(cancleChn)
	defer close(abortChn)
	defer close(execChn)

	stdout, _ := botCmd.StdoutPipe()
	stderr, _ := botCmd.StderrPipe()
	botCmd.Start()
	conn.Call("WorkerAPI.PublishTaskStarted", task, &ack)

	out := ""
	err := ""
	exit_code := 0
	go waitForCanclation(cancleChn, abortChn, botCmd)
	go execBot(execChn, botCmd, stdout, stderr, &out, &err, &exit_code)
	select {
	case <-cancleChn:
		// nop
	case <-execChn:
		abortChn <- true
		var patch_content string
		if task.Patch {
			in, err := ioutil.ReadFile(fmt.Sprintf("%s/%s/%d/changes.patch",
				cache_directory, patches_directory, task.Id))
			if err == nil {
				patch_content = string(in)
				os.RemoveAll(fmt.Sprintf("%s/%s/%d", cache_directory,
					patches_directory, task.Id))
			}
		}
		conn.Call("WorkerAPI.PublishTaskResult", remote_worker.Result{
			Tid:         task.Id,
			Stdout:      out,
			Stderr:      err,
			Exit_status: exit_code,
			Patch:       patch_content,
		}, &ack)
	}
}

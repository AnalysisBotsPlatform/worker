// Background worker.
package worker

import (
	"fmt"
	remote_worker "github.com/AnalysisBotsPlatform/platform/worker"
	"io"
	"io/ioutil"
	"net/rpc"
	"os"
	"os/exec"
	"os/user"
	"syscall"
)

// Directory (within the cache directory) where projects are found.
const projects_directory = "projects"
const project = "/project"

// Directory (within the cache directory) where patch files are found.
const patches_directory = "patches"
const patch = "/patch"

// Docker container resource limits.
const max_memory = "128m"
const max_cpus = "1"

// Directory where the application can store temporary data on the file system.
var cache_directory string

// Length of the directory names for the cloned GitHub projects.
// NOTE: If the platform gets many users, this number should be raised.
const directory_length = 8

// Connection to the Worker controller
var conn *rpc.Client

// Initialization of the worker. Sets up the cancelation channel, the cache
// directory is set and the connection is saved.
func Init(path_to_cache string, connection *rpc.Client) {
	cache_directory = path_to_cache
	conn = connection
}

// Waits on the channel for a cancelation action. If such an action is received,
// the corresponding process is terminated (Docker container) and the `execBot`
// function is able to continue its execution.
func waitForCanclation(returnChn, abortWait, cancel chan bool, cmd *exec.Cmd) {
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

// Run the Bot on the Project.
func run(task *remote_worker.Task, cancel chan bool,
	project_path, patch_path string) {
	var botCmdArgs []string
	var ack bool

	botCmdArgs = append(botCmdArgs, "run")
	// remove container after execution
	botCmdArgs = append(botCmdArgs, "--rm")
	// memory limit
	botCmdArgs = append(botCmdArgs, fmt.Sprintf("--memory=\"%s\"", max_memory))
	// cpu limit
	botCmdArgs = append(botCmdArgs, fmt.Sprintf("--cpuset-cpus=\"%s\"",
		max_cpus))
	// mount cloned project into container (read-only)
	botCmdArgs = append(botCmdArgs, "-v", fmt.Sprintf("%s:%s:ro", project_path,
		project))

	if task.Patch {
		// mount writeable directory into container
		// the patch file should be written therein
		botCmdArgs = append(botCmdArgs, "-v", fmt.Sprintf("%s:%s", patch_path,
			patch))
		// set the user id within the container to match the user that
		// executes the Worker
		current_user, _ := user.Current()
		botCmdArgs = append(botCmdArgs, "-u", current_user.Uid)
	}

	// Docker images that gets executed
	botCmdArgs = append(botCmdArgs, task.Bot)
	// 1st argument for the container (directory where the project clone
	// is located)
	botCmdArgs = append(botCmdArgs, project)

	if task.Patch {
		// 2nd argument for the container (file where the Git patch should be
		// written to)
		botCmdArgs = append(botCmdArgs, fmt.Sprintf("%s/changes.patch", patch))
	}

	// use "docker run" to execute the Bot
	botCmd := exec.Command("docker", botCmdArgs...)

	// channels used to synchronize
	cancleChn := make(chan bool)
	abortChn := make(chan bool)
	execChn := make(chan bool)
	defer close(cancleChn)
	defer close(abortChn)
	defer close(execChn)

	// start Bot
	stdout, _ := botCmd.StdoutPipe()
	stderr, _ := botCmd.StderrPipe()
	botCmd.Start()
	conn.Call("WorkerAPI.PublishTaskStarted", task, &ack)

	// wait for termination or cancelation
	out := ""
	err := ""
	exit_code := 0
	go waitForCanclation(cancleChn, abortChn, cancel, botCmd)
	go execBot(execChn, botCmd, stdout, stderr, &out, &err, &exit_code)

	select {
	case <-cancleChn:
		// nop
	case <-execChn:
		abortChn <- true

		// publish result
		var patch_content string
		if task.Patch {
			if in, err := ioutil.ReadFile(fmt.Sprintf("%s/changes.patch",
				patch_path)); err == nil {
				patch_content = string(in)
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

// Try to create a directory in the cache directory. Cancel the task if this
// fails.
func createDirectoryOrCancelTask(path string, tid int64,
	cancel chan bool) bool {
	var ack bool
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			conn.Call("WorkerAPI.PublishTaskResult", remote_worker.Result{
				Tid:         tid,
				Stdout:      "",
				Stderr:      "Cannot create cache directory!",
				Exit_status: -1,
			}, &ack)
			<-cancel
			return false
		}
	}
	return true
}

// Try run run a command. Cancel the task if this fails.
func runCommandOrCancelTask(cmd *exec.Cmd, tid int64, cancel chan bool) bool {
	var ack bool
	if output, err := cmd.CombinedOutput(); err != nil {
		err_msg := fmt.Sprintln(err)
		err_msg = fmt.Sprintf("%s\n%s", err_msg, output)
		conn.Call("WorkerAPI.PublishTaskResult", remote_worker.Result{
			Tid:         tid,
			Stdout:      "",
			Stderr:      err_msg,
			Exit_status: -1,
		}, &ack)
		<-cancel
		return false
	}
	return true
}

// Preperation steps:
// - Creates the project cache directory if nesessary.
// - Fetches the Bot from DockerHub.
// General:
// - Creates a new clone of the repository.
// NOTE: Later this may be changed to a pull instead of clone, i.e. a cloned
// repository is reused.
// - The Bot is executed on the cloned project. This includes the creation of a
// new Docker container from the Bot's Docker image.
// - Waits for completion of the Bot's execution.
// - For Bot's that have a Git patch support the patch is read and appended to
// the result data.
// - Cleans up project cache directory (removes clone).
// - Publishes results accordingly (this includes exit status and output)
func RunTask(task *remote_worker.Task, cancel chan bool) {
	// create project cache directory if necessary
	project_path := fmt.Sprintf("%s/%s/%d", cache_directory, projects_directory,
		task.Id)
	if !createDirectoryOrCancelTask(project_path, task.Id, cancel) {
		return
	}
	defer os.RemoveAll(project_path)

	// create patch cache directory if necessary
	var patch_path string
	if task.Patch {
		patch_path = fmt.Sprintf("%s/%s/%d", cache_directory, patches_directory,
			task.Id)
		if !createDirectoryOrCancelTask(patch_path, task.Id, cancel) {
			return
		}
		defer os.RemoveAll(patch_path)
	}

	// fetch Bot from DockerHub
	if !runCommandOrCancelTask(exec.Command("docker", "pull", task.Bot),
		task.Id, cancel) {
		// NOTE This should not happen. Either docker is not available or the
		// Bot was removed from the DockerHub. One might want to invalidate the
		// Bot in case err is an ExitError.
		return
	}
	select {
	case <-cancel:
		return
	default:
	}

	// clone project from GitHub and invalidate remote server
	if !runCommandOrCancelTask(exec.Command("git", "clone",
		fmt.Sprintf("https://%s@github.com/%s.git", task.GH_token,
			task.Project), project_path), task.Id, cancel) {
		return
	} else {
		cmd := exec.Command("git", "remote", "set-url", "origin", "0.0.0.0")
		cmd.Dir = project_path
		if !runCommandOrCancelTask(cmd, task.Id, cancel) {
			return
		}
	}
	select {
	case <-cancel:
		return
	default:
	}

	// run Bot on Project
	run(task, cancel, project_path, patch_path)
}

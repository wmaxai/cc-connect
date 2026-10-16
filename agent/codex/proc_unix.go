//go:build unix

package codex

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

func prepareCmdForKill(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// signalProcessGroup sends sig to the entire process group rooted at cmd.
// Returns nil if the group is already gone. Aiming at the group matters for
// `codex`: the CLI is often a thin wrapper that spawns the real app-server, so
// signalling only the direct child would leave the app-server alive as an
// orphan that keeps holding its per-thread writer locks.
func signalProcessGroup(cmd *exec.Cmd, sig syscall.Signal) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-cmd.Process.Pid, sig); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// gracefulStopCmd asks the whole process group to exit cleanly. codex deletes
// its thread writer locks and finishes the rollout it is writing on a clean
// exit, so the threads it served stay resumable straight away.
func gracefulStopCmd(cmd *exec.Cmd) error {
	return signalProcessGroup(cmd, syscall.SIGTERM)
}

func forceKillCmd(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return signalProcessGroup(cmd, syscall.SIGKILL)
}

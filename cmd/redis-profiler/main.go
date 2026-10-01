// Command redis-profiler runs Redis with gperftools CPU sampling and exposes
// a five-second profile over HTTP to the benchmark runner.
package main

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const profilePath = "/tmp/redis.cpu"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: redis-profiler redis-server [arguments]")
		os.Exit(2)
	}
	redis := exec.Command(os.Args[1], os.Args[2:]...)
	redis.Stdout, redis.Stderr = os.Stdout, os.Stderr
	redis.Env = append(os.Environ(), "LD_PRELOAD=libprofiler.so.0", "CPUPROFILE="+profilePath, fmt.Sprintf("CPUPROFILESIGNAL=%d", syscall.SIGWINCH))
	if err := redis.Start(); err != nil {
		panic(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-signals; _ = redis.Process.Signal(syscall.SIGTERM) }()
	go func() {
		if err := redis.Wait(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}()

	var mu sync.Mutex
	http.HandleFunc("/debug/profile", func(w http.ResponseWriter, r *http.Request) {
		seconds, err := strconv.Atoi(r.URL.Query().Get("seconds"))
		if err != nil || seconds < 1 || seconds > 30 {
			http.Error(w, "seconds must be between 1 and 30", http.StatusBadRequest)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		old, _ := filepath.Glob(profilePath + ".*")
		for _, path := range old {
			_ = os.Remove(path)
		}
		if err := redis.Process.Signal(syscall.SIGWINCH); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		time.Sleep(time.Duration(seconds) * time.Second)
		if err := redis.Process.Signal(syscall.SIGWINCH); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for i := 0; i < 20; i++ {
			files, _ := filepath.Glob(profilePath + ".*")
			var newest string
			var newestStat os.FileInfo
			for _, path := range files {
				stat, err := os.Stat(path)
				if err != nil || stat.Size() == 0 {
					continue
				}
				if newestStat == nil || stat.ModTime().After(newestStat.ModTime()) {
					newest, newestStat = path, stat
				}
			}
			if newest != "" {
				time.Sleep(50 * time.Millisecond)
				stable, err := os.Stat(newest)
				if err == nil && stable.Size() == newestStat.Size() {
					http.ServeFile(w, r, newest)
					return
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		http.Error(w, "Redis CPU profile was not written", http.StatusInternalServerError)
	})
	http.HandleFunc("/debug/binary", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "/usr/local/bin/redis-server")
	})
	if err := http.ListenAndServe(":6060", nil); err != nil {
		panic(err)
	}
}

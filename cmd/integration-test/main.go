package main

import (
	"chukcha/client"
	"errors"
	"fmt"
	"go/build"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	maxN          = 10000000
	maxBufferSize = 1024 * 1024

	sendFmt = "Send: net %13s, cpu %13s (%.1f MiB)"
	recvFmt = "Recv: net %13s, cpu %13s"
)

func main() {
	if err := runTest(); err != nil {
		log.Fatalf("Test failed: %v", err)
	}
	log.Printf("Test passed!")
}
func runTest() error {
	// time info
	log.SetFlags(log.Flags() | log.Lmicroseconds)

	goPath := os.Getenv("GOPATH")
	if goPath == "" {
		goPath = build.Default.GOPATH
	}
	log.Printf("Compiling chukcha")
	out, err := exec.Command("go", "install", "-v", "/root/wk/chukcha").CombinedOutput()
	if err != nil {
		log.Printf("Failed to build: %v", err)
		return fmt.Errorf("compilation failed: %v (out: %s)", err, string(out))
	}
	port := 8089
	dbPath := "/tmp/chukcha"
	os.RemoveAll(dbPath)
	os.Mkdir(dbPath, 0777)
	if isPortInUse(port) {
		err := killProcessByPort(port)
		if err != nil {
			return fmt.Errorf("kill Process by port err: %v", err)
		}
	}

	os.WriteFile("/tmp/chukcha/chunk1", []byte("12345\n"), 0666)
	log.Printf("Running chukcha on port %d", port)

	cmd := exec.Command(goPath+"/bin/chukcha", "-dirname="+dbPath, fmt.Sprintf("-port=%d", port))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		log.Printf("Failed to start process: %v", err)
		return fmt.Errorf("failed to start chukcha: %v", err)
	}

	defer func() {
		cmd.Process.Kill()
	}()
	log.Printf("Waiting for the port localhost:%d to open", port)
	for i := 0; i <= 100; i++ {
		timeout := time.Millisecond * 50
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", fmt.Sprint(port)), timeout)
		if err != nil {
			time.Sleep(timeout)
			continue
		}
		conn.Close()
		break
	}
	log.Printf("Starting the test")
	s := client.NewSimple([]string{fmt.Sprintf("http://localhost:%d", port)})
	want, get, err := sendAndReceiveConcurrently(s)
	if err != nil {
		return fmt.Errorf("sendAndReceiveConcurrently: %v", err)
	}
	want += 12345
	if want != get {
		log.Fatalf("The expected sum %d is not equal to the actual sum %d", want, get)
	}
	log.Printf("The test pass")
	return nil
}

type sumAndErr struct {
	sum int64
	err error
}

func sendAndReceiveConcurrently(s *client.Simple) (want, get int64, err error) {
	wantCh := make(chan sumAndErr, 1)
	getCh := make(chan sumAndErr, 1)
	sendFinishedCh := make(chan bool, 1)
	go func() {
		want, err := send(s)
		wantCh <- sumAndErr{
			sum: want,
			err: err,
		}
		sendFinishedCh <- true
	}()
	go func() {
		get, err = receive(s, sendFinishedCh)
		getCh <- sumAndErr{
			sum: get,
			err: err,
		}
	}()
	wantRes := <-wantCh
	if wantRes.err != nil {
		return 0, 0, fmt.Errorf("send: %v", err)
	}
	getRes := <-getCh
	if wantRes.err != nil {
		return 0, 0, fmt.Errorf("receive: %v", err)
	}
	return wantRes.sum, getRes.sum, err
}

func killProcessByPort(port int) error {
	cmd := exec.Command("lsof", "-t", fmt.Sprintf("-i:%d", port))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("list pid failed :%v", err)
	}
	pids := strings.Split(string(out), "\n")
	for _, pid := range pids {
		if pid != "" {
			log.Printf("killing process %s", pid)
			killCmd := exec.Command("kill", "-9", pid)
			killCmd.Run()
		}
	}
	return nil
}
func isPortInUse(port int) bool {
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
func send(s *client.Simple) (sum int64, err error) {
	sendStart := time.Now()
	var networkTime time.Duration
	var sentBytes int
	defer func() {
		log.Printf(sendFmt, networkTime, time.Since(sendStart)-networkTime, float64(sentBytes/1024/1024))
	}()
	buf := make([]byte, 0, maxBufferSize)
	for i := 0; i <= maxN; i++ {
		sum += int64(i)

		buf = strconv.AppendInt(buf, int64(i), 10)
		buf = append(buf, '\n')

		if len(buf) >= maxBufferSize {
			start := time.Now()
			if err := s.Send(buf); err != nil {
				return 0, err
			}
			networkTime += time.Since(start)
			sentBytes += len(buf)
			buf = buf[0:0]
		}
	}
	if len(buf) != 0 {
		start := time.Now()
		if err := s.Send(buf); err != nil {
			return 0, err
		}
		networkTime += time.Since(start)
		sentBytes += len(buf)
	}
	return sum, nil
}

func receive(s *client.Simple, sendFinishedCh chan bool) (sum int64, err error) {
	buf := make([]byte, maxBufferSize)

	var parseTime time.Duration
	receiveStart := time.Now()
	defer func() {
		log.Printf(recvFmt, time.Since(receiveStart)-parseTime, parseTime)
	}()
	trimNL := func(r rune) bool {
		return r == '\n'
	}
	sendFinished := false
	for {
		select {
		case <-sendFinishedCh:
			sendFinished = true
		default:
		}

		res, err := s.Receive(buf)
		if errors.Is(err, io.EOF) {
			if sendFinished {
				log.Printf("Receive: get information that send finished")
				return sum, nil
			}
			time.Sleep(time.Millisecond * 10)
			continue
		} else if err != nil {
			return 0, err
		}
		start := time.Now()
		ints := strings.Split(strings.TrimRightFunc(string(res), trimNL), "\n")
		for _, str := range ints {
			i, err := strconv.Atoi(str)
			if err != nil {
				return 0, err
			}
			sum += int64(i)
		}
		parseTime += time.Since(start)
	}
}

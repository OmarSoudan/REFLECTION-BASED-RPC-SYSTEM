package rpc

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

type testService struct {
	slowStarted chan struct{}
}

func (*testService) Ping(id int) (string, error) {
	if id < 0 {
		return "", errors.New("id must be non-negative")
	}
	return "Pong" + string(rune('0'+id)), nil
}

func (*testService) Add(left, right int) int { return left + right }

func (*testService) Panic() (string, error) { panic("test panic") }

func (service *testService) Slow() string {
	if service.slowStarted != nil {
		close(service.slowStarted)
	}
	time.Sleep(150 * time.Millisecond)
	return "done"
}

func (*testService) NoArgs() string { return "ready" }

func (*testService) NoResult() {}

func (*testService) Echo(value string) string { return value }

func (*testService) OnlyError(shouldFail bool) error {
	if shouldFail {
		return errors.New("requested failure")
	}
	return nil
}

func TestValidateRequest(t *testing.T) {
	largeArgument := make(json.RawMessage, maxArgumentBytes+1)
	tests := []struct {
		name    string
		request request
		wantErr string
	}{
		{name: "empty method", request: request{}, wantErr: "method cannot be empty"},
		{name: "space in method", request: request{Method: "Bad Method"}, wantErr: "whitespace"},
		{name: "control character", request: request{Method: "Bad\nMethod"}, wantErr: "whitespace"},
		{name: "too many arguments", request: request{Method: "Add", Args: make([]json.RawMessage, maxArguments+1)}, wantErr: "too many arguments"},
		{name: "large argument", request: request{Method: "Add", Args: []json.RawMessage{largeArgument}}, wantErr: "larger"},
		{name: "large request", request: request{Method: "Add", Args: []json.RawMessage{make(json.RawMessage, maxArgumentBytes-1), make(json.RawMessage, maxArgumentBytes-1), make(json.RawMessage, maxArgumentBytes-1), make(json.RawMessage, maxArgumentBytes-1), make(json.RawMessage, maxArgumentBytes-1)}}, wantErr: "larger"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateRequest(test.request)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("validateRequest() error = %v, want text %q", err, test.wantErr)
			}
		})
	}
	for _, valid := range []request{{Method: "Ping"}, {Method: "Add", Args: []json.RawMessage{json.RawMessage("1"), json.RawMessage("2")}}} {
		if err := validateRequest(valid); err != nil {
			t.Fatalf("valid request rejected: %v", err)
		}
	}
}

func TestServerClient(t *testing.T) {
	server, err := NewServer(&testService{})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	client, err := NewClient(server.Address())
	if err != nil {
		t.Fatal(err)
	}
	var sum int
	if err := client.Call("Add", &sum, 20, 22); err != nil {
		t.Fatal(err)
	}
	if sum != 42 {
		t.Fatalf("got %d, want 42", sum)
	}
	var pong string
	if err := client.Call("Ping", &pong, 4); err != nil {
		t.Fatal(err)
	}
	if pong != "Pong4" {
		t.Fatalf("got %q, want Pong4", pong)
	}
	if err := client.Call("Ping", &pong, -1); err == nil {
		t.Fatal("expected remote error")
	}
}

func TestServerClientMethodShapes(t *testing.T) {
	server := startTestServer(t)
	client := testClient(t, server)

	var noArgs string
	if err := client.Call("NoArgs", &noArgs); err != nil {
		t.Fatal(err)
	}
	if noArgs != "ready" {
		t.Fatalf("NoArgs() = %q, want %q", noArgs, "ready")
	}

	if err := client.Call("NoResult", nil); err != nil {
		t.Fatal(err)
	}

	var echoed string
	if err := client.Call("Echo", &echoed, "hello"); err != nil {
		t.Fatal(err)
	}
	if echoed != "hello" {
		t.Fatalf("Echo() = %q, want %q", echoed, "hello")
	}

	if err := client.Call("OnlyError", nil, false); err != nil {
		t.Fatal(err)
	}
	if err := client.Call("OnlyError", nil, true); err == nil || !strings.Contains(err.Error(), "requested failure") {
		t.Fatalf("OnlyError(true) error = %v, want requested failure", err)
	}
}

func TestInvalidRemoteCallsBecomeErrors(t *testing.T) {
	server := startTestServer(t)
	client := testClient(t, server)

	tests := []struct {
		name   string
		method string
		out    any
		args   []any
		want   string
	}{
		{name: "unknown method", method: "Missing", want: "unknown method"},
		{name: "wrong argument count", method: "Add", args: []any{1}, want: "expects 2 arguments"},
		{name: "invalid argument JSON type", method: "Add", args: []any{"left", 2}, want: "argument 0"},
		{name: "invalid output type", method: "Add", out: new(string), args: []any{1, 2}, want: "decode"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := client.Call(test.method, test.out, test.args...)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Call() error = %v, want text %q", err, test.want)
			}
		})
	}
}

func TestRemotePanicBecomesError(t *testing.T) {
	server := startTestServer(t)
	client := testClient(t, server)
	var result string
	err := client.Call("Panic", &result)
	if err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("panic error = %v, want remote panic error", err)
	}
}

func TestConcurrentCalls(t *testing.T) {
	server := startTestServer(t)
	client := testClient(t, server)
	errors := make(chan error, 20)
	for index := 0; index < 20; index++ {
		go func(value int) {
			var result int
			errors <- client.Call("Add", &result, value, value)
		}(index)
	}
	for index := 0; index < 20; index++ {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
}

func TestStopAndRestart(t *testing.T) {
	server, err := NewServer(&testService{})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
	if server.Address() != "" {
		t.Fatalf("address after stop = %q", server.Address())
	}
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	if server.Address() == "" {
		t.Fatal("restart did not assign an address")
	}
}

func TestServerLifecycleErrors(t *testing.T) {
	server, err := NewServer(&testService{})
	if err != nil {
		t.Fatal(err)
	}
	if got := server.Address(); got != "" {
		t.Fatalf("Address before Start = %q, want empty", got)
	}
	if err := server.Stop(); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	if err := server.Start("127.0.0.1:0"); err == nil {
		t.Fatal("second Start succeeded, want error")
	}
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
	if got := server.Address(); got != "" {
		t.Fatalf("Address after Stop = %q, want empty", got)
	}
	if err := server.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

func TestStopWaitsForActiveHandler(t *testing.T) {
	service := &testService{slowStarted: make(chan struct{})}
	server, err := NewServer(service)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })
	client := testClient(t, server)
	done := make(chan error, 1)
	go func() {
		var result string
		done <- client.Call("Slow", &result)
	}()

	select {
	case <-service.slowStarted:
	case <-time.After(time.Second):
		t.Fatal("Slow handler did not start")
	}
	start := time.Now()
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("Stop returned after %v, want it to wait for active handler", elapsed)
	}
	if err := <-done; err != nil {
		t.Fatalf("active call failed while stopping: %v", err)
	}
}

func TestClientTimeout(t *testing.T) {
	server := startTestServer(t)
	client := testClient(t, server)
	client.Timeout = 20 * time.Millisecond
	var result string
	if err := client.Call("Slow", &result); err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestClientConnectionFailures(t *testing.T) {
	client, err := NewClient("127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	client.Timeout = 50 * time.Millisecond
	var result string
	if err := client.Call("NoArgs", &result); err == nil {
		t.Fatal("Call to unavailable server succeeded")
	}
}

func TestMalformedRequestGetsErrorResponse(t *testing.T) {
	server := startTestServer(t)
	connection, err := net.Dial("tcp", server.Address())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.Write([]byte("not json\n")); err != nil {
		t.Fatal(err)
	}
	var result response
	if err := json.NewDecoder(bufio.NewReader(connection)).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Error == "" {
		t.Fatal("malformed request returned no error")
	}
}

func TestOversizedRequestGetsErrorResponse(t *testing.T) {
	server := startTestServer(t)
	connection, err := net.Dial("tcp", server.Address())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()

	argument := strings.Repeat("x", maxArgumentBytes)
	request := `{"method":"Echo","args":["` + argument + `"]}` + "\n"
	if _, err := connection.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	var result response
	if err := json.NewDecoder(bufio.NewReader(connection)).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Error == "" {
		t.Fatal("oversized request returned no error")
	}
}

func startTestServer(t *testing.T) *Server {
	t.Helper()
	server, err := NewServer(&testService{})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })
	return server
}

func testClient(t *testing.T, server *Server) *Client {
	t.Helper()
	client, err := NewClient(server.Address())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

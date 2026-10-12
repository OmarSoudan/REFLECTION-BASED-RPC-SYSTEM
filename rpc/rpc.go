// Package rpc is an intentionally incomplete RMI assignment template.
package rpc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	maxArguments       = 8
	maxArgumentBytes   = 1 << 20
	maxRequestBytes    = 4 << 20
	defaultCallTimeout = 5 * time.Second
)

type request struct {
	Method string            `json:"method"`
	Args   []json.RawMessage `json:"args"`
}

type response struct {
	OK    bool            `json:"ok"`
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
}

// Server exposes exported methods of a receiver over TCP.
type Server struct {
	receiver   any
	listener   net.Listener
	stopped    chan struct{}
	acceptDone chan struct{}
	workers    sync.WaitGroup
	mu         sync.Mutex
}

// NewServer creates a server for a non-nil pointer receiver.
func NewServer(receiver any) (*Server, error) {
	if receiver == nil {
		return nil, errors.New("receiver cannot be nil")
	}
	value := reflect.ValueOf(receiver)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, errors.New("receiver must be a non-nil pointer")
	}
	return &Server{receiver: receiver}, nil
}

// TODO 1: Create the TCP listener, reject duplicate starts, save lifecycle state,
// and launch the accept loop without blocking the caller.
// Start begins accepting remote calls at address. Use port 0 to request an available port.
func (s *Server) Start(address string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener != nil {
		return errors.New("server is already running")
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	s.listener = listener
	s.stopped = make(chan struct{})
	s.acceptDone = make(chan struct{})

	go func() {
		defer close(s.acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-s.stopped:
					return
				default:
					return
				}
			}

			s.workers.Add(1)
			go func(c net.Conn) {
				defer s.workers.Done()
				s.handle(c)
			}(conn)
		}
	}()

	return nil
}

// TODO 2: Return the listener address safely while allowing port 0 discovery.
// Address returns the address assigned to the running server.
func (s *Server) Address() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// TODO 3: Close the listener, signal the accept loop, wait for its workers,
// and leave the server in a state where Start can be called again.
// Stop stops accepting new connections and waits for active handlers to finish.
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.listener == nil {
		s.mu.Unlock()
		return nil
	}

	close(s.stopped)
	err := s.listener.Close()
	acceptDone := s.acceptDone
	s.listener = nil
	s.mu.Unlock()

	<-acceptDone
	s.workers.Wait()

	return err
}

// TODO 4: Study this starter request handler, then complete and test the protocol
// contract. Add tests for malformed JSON, oversized input, remote errors, panics,
// and successful calls, and make sure one bad request cannot crash the server or
// leave a connection hanging. Connect this handler to your completed Start and
// Stop lifecycle so each accepted connection is processed by a worker safely.
// handle reads one request, invokes it, and writes one response.
func (s *Server) handle(connection net.Conn) {
	defer connection.Close()
	encoder := json.NewEncoder(connection)
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = encoder.Encode(response{Error: fmt.Sprintf("remote method panicked: %v", recovered)})
		}
	}()

	decoder := json.NewDecoder(bufio.NewReader(io.LimitReader(connection, maxRequestBytes+1)))
	var req request
	if err := decoder.Decode(&req); err != nil {
		_ = encoder.Encode(response{Error: fmt.Sprintf("invalid request: %v", err)})
		return
	}
	if err := validateRequest(req); err != nil {
		_ = encoder.Encode(response{Error: err.Error()})
		return
	}
	result, err := s.invoke(req)
	if err != nil {
		_ = encoder.Encode(response{Error: err.Error()})
		return
	}
	payload, err := json.Marshal(result)
	if err != nil {
		_ = encoder.Encode(response{Error: fmt.Sprintf("encode result: %v", err)})
		return
	}
	_ = encoder.Encode(response{OK: true, Value: payload})
}

// TODO 5: Reject empty or unsafe method names, too many arguments, arguments
// larger than maxArgumentBytes, and requests larger than maxRequestBytes.
// validateRequest rejects malformed or abusive requests before reflection runs.
func validateRequest(req request) error {
	if req.Method == "" {
		return errors.New("method cannot be empty")
	}

	for _, r := range req.Method {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return errors.New("method name contains whitespace or control characters")
		}
	}

	if len(req.Args) > maxArguments {
		return errors.New("too many arguments")
	}

	totalBytes := 0
	for _, arg := range req.Args {
		if len(arg) > maxArgumentBytes {
			return errors.New("argument larger than limit")
		}
		totalBytes += len(arg)
	}

	if totalBytes > maxRequestBytes {
		return errors.New("request larger than limit")
	}

	return nil
}

// TODO 6: Study and test this starter reflection bridge. Your required work is to
// support and test exported methods with zero arguments, typed arguments, no
// return value, one return value, and a final error return; unknown methods,
// wrong argument counts, invalid JSON arguments, returned errors, and panics must
// become clear errors instead of crashing the server. Extend the bridge only when
// your chosen assignment protocol requires another result or argument rule.
// invoke finds a method, converts JSON arguments to its parameter types, and calls it.
func (s *Server) invoke(req request) (any, error) {
	method := reflect.ValueOf(s.receiver).MethodByName(req.Method)
	if !method.IsValid() {
		return nil, fmt.Errorf("unknown method %q", req.Method)
	}
	typeOfMethod := method.Type()
	if typeOfMethod.NumIn() != len(req.Args) {
		return nil, fmt.Errorf("method %q expects %d arguments, got %d", req.Method, typeOfMethod.NumIn(), len(req.Args))
	}
	arguments := make([]reflect.Value, len(req.Args))
	for index, raw := range req.Args {
		argument := reflect.New(typeOfMethod.In(index))
		if err := json.Unmarshal(raw, argument.Interface()); err != nil {
			return nil, fmt.Errorf("argument %d: %v", index, err)
		}
		arguments[index] = argument.Elem()
	}
	outputs, err := callMethodSafely(method, arguments)
	if err != nil {
		return nil, err
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	if last := outputs[len(outputs)-1]; last.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		if !isNilValue(last) {
			return nil, last.Interface().(error)
		}
		outputs = outputs[:len(outputs)-1]
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	return outputs[0].Interface(), nil
}

func isNilValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func callMethodSafely(method reflect.Value, arguments []reflect.Value) (outputs []reflect.Value, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("remote method panicked: %v", recovered)
		}
	}()
	return method.Call(arguments), nil
}

// Client calls methods on a remote Server.
type Client struct {
	Address string
	Timeout time.Duration
}

// NewClient creates a client for a server address.
func NewClient(address string) (*Client, error) {
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}
	return &Client{Address: address, Timeout: defaultCallTimeout}, nil
}

// TODO 7: Open one connection, apply a deadline, encode the request, read one
// response, return remote errors, and decode the response into out.
// Call invokes method and decodes its result into out. Pass nil for no result.
func (c *Client) Call(method string, out any, args ...any) error {
	if strings.TrimSpace(method) == "" {
		return errors.New("method cannot be empty")
	}

	dialer := net.Dialer{Timeout: c.Timeout}
	conn, err := dialer.Dial("tcp", c.Address)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	defer conn.Close()

	if c.Timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(c.Timeout))
	}

	rawArgs := make([]json.RawMessage, len(args))
	for i, arg := range args {
		marshaled, err := json.Marshal(arg)
		if err != nil {
			return fmt.Errorf("failed to encode argument %d: %w", i, err)
		}
		rawArgs[i] = marshaled
	}

	req := request{
		Method: method,
		Args:   rawArgs,
	}

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	var resp response
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if resp.Error != "" {
		return errors.New(resp.Error)
	}

	if out != nil && len(resp.Value) > 0 {
		if err := json.Unmarshal(resp.Value, out); err != nil {
			return fmt.Errorf("failed to decode response value: %w", err)
		}
	}

	return nil
}
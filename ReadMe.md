================================================================================
LAB 1: BUILDING A REFLECTION-BASED RPC SYSTEM
================================================================================

1. PROJECT OVERVIEW
--------------------------------------------------------------------------------
This project implements a lightweight Remote Procedure Call (RPC) framework in 
Go over TCP using JSON serialization and runtime reflection.

Key Architectural Components:
- Protocol: Clients issue JSON requests containing a target method name and an 
  array of encoded arguments. The server replies with a JSON envelope containing 
  success status ("ok"), an optional result payload ("value"), or an error 
  string ("error").
- Transport: TCP connections where each remote procedure invocation opens a 
  connection, exchanges a single request-response cycle, and cleanly closes it.
- Dynamic Dispatch: Uses Go's `reflect` package to look up exported methods on 
  a service struct at runtime, validate parameter lengths and types, convert 
  JSON inputs into native Go types, invoke the function, and safely capture 
  returned values, errors, or panics.
- Server Lifecycle & Concurrency: Supports thread-safe start, stop, and restart 
  routines, dynamic port assignment (port 0), background accept loops, and 
  graceful drain of active request workers via WaitGroups before server shutdown.
- Request Sanitization: Rejects abusive or malformed requests (empty names, 
  whitespace/control characters, argument count limits, payload byte limits) 
  before reflection is invoked.


2. PROJECT STRUCTURE
--------------------------------------------------------------------------------
lab1/
├── cmd/
│   └── ping/
│       └── main.go       # Demo client and server using the RPC package
├── rpc/
│   ├── rpc.go            # Full RPC implementation (Tasks 1 to 7)
│   └── rpc_test.go       # Comprehensive test suite covering all cases
├── go.mod                # Module definition file
└── README.md             # Project & team details (optional)


3. PREREQUISITES
--------------------------------------------------------------------------------
- Go 1.20 or newer installed and accessible via system PATH.
- Verify installation:
    go version


4. HOW TO RUN
--------------------------------------------------------------------------------
Open a terminal in the root project directory (`lab1`):

A. Run the Automated Test Suite:
   To run all unit, lifecycle, and validation tests:
       go test -v ./...

B. Run the Ping Demonstration Application:
   To run the client-server ping demonstration:
       go run ./cmd/ping


5. WHAT TO EXPECT (OUTPUTS)
--------------------------------------------------------------------------------
A. Running the Test Suite (`go test -v ./...`):
   All test cases should pass without panics, race conditions, or failures:

   ?       lab1/cmd/ping   [no test files]
   === RUN   TestValidateRequest
   === RUN   TestValidateRequest/empty_method
   === RUN   TestValidateRequest/space_in_method
   === RUN   TestValidateRequest/control_character
   === RUN   TestValidateRequest/too_many_arguments
   === RUN   TestValidateRequest/large_argument
   === RUN   TestValidateRequest/large_request
   --- PASS: TestValidateRequest (0.00s)
   === RUN   TestServerClient
   --- PASS: TestServerClient (0.00s)
   === RUN   TestServerClientMethodShapes
   --- PASS: TestServerClientMethodShapes (0.00s)
   === RUN   TestInvalidRemoteCallsBecomeErrors
   --- PASS: TestInvalidRemoteCallsBecomeErrors (0.00s)
   === RUN   TestRemotePanicBecomesError
   --- PASS: TestRemotePanicBecomesError (0.00s)
   === RUN   TestConcurrentCalls
   --- PASS: TestConcurrentCalls (0.00s)
   === RUN   TestStopAndRestart
   --- PASS: TestStopAndRestart (0.00s)
   === RUN   TestServerLifecycleErrors
   --- PASS: TestServerLifecycleErrors (0.00s)
   === RUN   TestStopWaitsForActiveHandler
   --- PASS: TestStopWaitsForActiveHandler (0.15s)
   === RUN   TestClientTimeout
   --- PASS: TestClientTimeout (0.15s)
   === RUN   TestClientConnectionFailures
   --- PASS: TestClientConnectionFailures (0.00s)
   === RUN   TestMalformedRequestGetsErrorResponse
   --- PASS: TestMalformedRequestGetsErrorResponse (0.00s)
   === RUN   TestOversizedRequestGetsErrorResponse
   --- PASS: TestOversizedRequestGetsErrorResponse (0.01s)
   PASS
   ok      lab1/rpc        0.331s

B. Running the Ping Demo (`go run ./cmd/ping`):
   The server starts on an ephemeral port (port 0), the client calls the
   "Ping" method 4 times sequentially, prints each reply, and terminates:

   Pong1
   Pong2
   Pong3
   Pong4


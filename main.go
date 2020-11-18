package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type RuntimeOptions struct {
	listenAddress  *net.TCPAddr
	connectAddress *net.TCPAddr
}

func parseFlags() RuntimeOptions {
	listenAddressStr := flag.String("listen", "", "the address:port or :port to listen on.")
	connectAddressStr := flag.String("connect", "", "the address:port or :port to connect to.")
	flag.Parse()

	if listenAddressStr == nil || *listenAddressStr == "" {
		fmt.Fprintf(os.Stderr, "-listen must be specified and cannot be empty\n")
		flag.Usage()
		os.Exit(2)
	}

	if connectAddressStr == nil || *connectAddressStr == "" {
		fmt.Fprintf(os.Stderr, "-connect must be specified and cannot be empty\n")
		flag.Usage()
		os.Exit(2)
	}

	listenAddress, err := net.ResolveTCPAddr("tcp", *listenAddressStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid -listen address: %v\n", err)
		os.Exit(2)
	}

	connectAddress, err := net.ResolveTCPAddr("tcp", *connectAddressStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid -connect address: %v\n", err)
		os.Exit(2)
	}

	return RuntimeOptions{listenAddress: listenAddress, connectAddress: connectAddress}
}

type IncomingConnection struct {
	Connection *net.TCPConn
	Error      error
}

func HandleIncomingConnections(listener *net.TCPListener, iccChan chan IncomingConnection) {
	for {
		conn, err := listener.AcceptTCP()
		iccChan <- IncomingConnection{Connection: conn, Error: err}

		if err != nil {
			close(iccChan)
			return
		}
	}
}

func HandleConnection(remote *net.TCPConn, connAddr *net.TCPAddr, connID uint64) {
	log.Printf("[%d] Connection from remote system %s", connID, remote.RemoteAddr().String())
	start := time.Now()
	local, err := net.DialTCP("tcp", nil, connAddr)
	end := time.Now()

	if err != nil {
		log.Printf("[%d] Failed to create local connection: %v", connID, err)
		log.Printf("[%d] Hanging up on remote system %s without reading any data", connID, remote.RemoteAddr().String())
		remote.Close()
		return
	}

	defer remote.Close()
	defer local.Close()

	log.Printf("[%d] Connected to local system %s in %d microseconds", connID, local.RemoteAddr().String(), end.Sub(start)/time.Microsecond)

	doneChan := make(chan bool, 2)

	go HandleFlow(remote, local, connID, "remote", "local", doneChan)
	go HandleFlow(local, remote, connID, "local", "remote", doneChan)

	for i := 0; i < 2; i++ {
		<-doneChan
	}

	log.Printf("[%d] Connection flows completed", connID)
}

func HandleFlow(reader *net.TCPConn, writer *net.TCPConn, connID uint64, readerName string, writerName string, doneChan chan bool) {
	buffer := make([]byte, 65536)
	var totalRead uint64
	var totalWritten uint64

flowLoop:
	for {
		nRead, rErr := reader.Read(buffer)
		if nRead > 0 {
			totalRead += uint64(nRead)

			for writeStart := 0; writeStart < nRead; {
				nWritten, wErr := writer.Write(buffer[writeStart:nRead])

				if nWritten > 0 {
					writeStart += nWritten
					totalWritten += uint64(nWritten)
				}

				if nWritten == 0 || wErr != nil {
					log.Printf("[%d] Failed to write %d bytes to %s: %v", connID, nRead, writerName, wErr)
					break flowLoop
				}
			}
		}

		if nRead == 0 || rErr == io.EOF {
			log.Printf("[%d] %s sent EOF", connID, readerName)
			break flowLoop
		}

		if rErr != nil {
			log.Printf("[%d] Encountered an error reading from %s: %v", connID, readerName, rErr)
			break flowLoop
		}
	}

	if err := writer.CloseWrite(); err != nil {
		log.Printf("[%d] Failed to close %s for writes: %v", connID, writerName, err)
	}

	log.Printf("[%d] Transferred %d/%d byte(s) from %s to %s", connID, totalWritten, totalRead, readerName, writerName)
	doneChan <- true
}

func main() {
	rtOpts := parseFlags()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC | log.Lshortfile)
	log.SetOutput(os.Stderr)

	log.Printf("Listening on %s", rtOpts.listenAddress.String())
	listener, err := net.ListenTCP("tcp", rtOpts.listenAddress)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to listen on %s: %v\n", rtOpts.listenAddress.String(), err)
		os.Exit(1)
	}

	defer listener.Close()
	iccChan := make(chan IncomingConnection, 1)
	exitChan := make(chan os.Signal, 1)

	// Capture SIGTERM, SIGINT
	signal.Notify(exitChan, syscall.SIGTERM, os.Interrupt)
	go HandleIncomingConnections(listener, iccChan)

	var nextConnectionID uint64

	log.Printf("Entering listen loop")
listenLoop:
	for {
		select {
		case sig := <-exitChan:
			log.Printf("Received signal %s", sig.String())
			listener.Close()
			break listenLoop

		case icc := <-iccChan:
			if icc.Error != nil {
				fmt.Fprintf(os.Stderr, "Failed to accept connection: %v\n", icc.Error)
				break listenLoop
			}

			log.Printf("Spawning connection for %s", icc.Connection.RemoteAddr().String())
			go HandleConnection(icc.Connection, rtOpts.connectAddress, nextConnectionID)
			nextConnectionID++
		}
	}
}

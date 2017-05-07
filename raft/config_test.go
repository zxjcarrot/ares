package raft

import (
	"fmt"
	"strings"
	"testing"
)

func TestReadConfigStream1(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || strings.Contains(r.(string), "coincides") == false {
			t.Fatalf("expecting 'coincides', got %v", r)
		} else {
			fmt.Printf("  ... Passed\n")
		}
	}()
	fmt.Printf("Test: Port Collision \n")
	configStream := `raft.self = localhost:12345:0
raft.peers = localhost:12345:0,localhost:12345:1,localhost:12347:2
`

	ParseConfigStream(strings.NewReader(configStream))

}

func TestReadConfigStream2(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || strings.Contains(r.(string), "missing raft.self field") == false {
			t.Fatalf("expecting 'missing self field', got '%v'", r)
		} else {
			fmt.Printf("  ... Passed\n")
		}
	}()
	fmt.Printf("Test: Missing self \n")
	configStream := `#raft.self = localhost:12345:0
raft.peers = localhost:12345:0,localhost:12345:1,localhost:12347:2
`

	ParseConfigStream(strings.NewReader(configStream))
}

func TestReadConfigStream3(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || strings.Contains(r.(string), "missing raft.peers field") == false {
			t.Fatalf("expecting 'missing peers field', got '%v'", r)
		} else {
			fmt.Printf("  ... Passed\n")
		}
	}()
	fmt.Printf("Test: Missing peers \n")
	configStream := `raft.self = localhost:12345:0
#raft.peers = localhost:12345:0,localhost:12345:1,localhost:12347:2
`

	ParseConfigStream(strings.NewReader(configStream))
}

func TestReadConfigStream4(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || strings.Contains(r.(string), "contains no self identity") == false {
			t.Fatalf("expecting 'contains no self identity', got '%v'", r)
		} else {
			fmt.Printf("  ... Passed\n")
		}
	}()
	fmt.Printf("Test: No self in peers \n")
	configStream := `raft.self = localhost:12345:0
raft.peers = localhost:12345:1,localhost:12347:2
`

	ParseConfigStream(strings.NewReader(configStream))
}

func TestReadConfigStream5(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || strings.Contains(r.(string), "unrecognized format") == false {
			t.Fatalf("expecting 'unrecognized format', got '%v'", r)
		} else {
			fmt.Printf("  ... Passed\n")
		}
	}()
	fmt.Printf("Test: Invalid Line Format \n")
	configStream := `raft.self == localhost:12345:0 =
`

	ParseConfigStream(strings.NewReader(configStream))
}
func TestReadConfigStream6(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || strings.Contains(r.(string), "PeerInfo") == false {
			t.Fatalf("expecting 'peerInfo', got '%v'", r)
		} else {
			fmt.Printf("  ... Passed\n")
		}
	}()
	fmt.Printf("Test: Invalid peerInfo 1\n")
	configStream := `raft.self = localhost::0
raft.peers = localhost::0,localhost:12345:1,localhost:12347:2
`

	ParseConfigStream(strings.NewReader(configStream))
}

func TestReadConfigStream7(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || strings.Contains(r.(string), "PeerInfo") == false {
			t.Fatalf("expecting 'peerInfo', got '%v'", r)
		} else {
			fmt.Printf("  ... Passed\n")
		}
	}()
	fmt.Printf("Test: Invalid peerInfo 2\n")
	configStream := `raft.self = localhost:12345:0
raft.peers = localhost:12345:0,localhost::1,localhost:12347:2
`

	ParseConfigStream(strings.NewReader(configStream))
}

func TestReadConfigStream8(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || strings.Contains(r.(string), "storageType") == false {
			t.Fatalf("expecting 'storageType', got '%v'", r)
		} else {
			fmt.Printf("  ... Passed\n")
		}
	}()
	fmt.Printf("Test: Invalid storageType\n")
	configStream := `raft.self = localhost:12345:0
raft.peers = localhost:12345:0,localhost:12346:1,localhost:12347:2
raft.storageType = balah
`

	ParseConfigStream(strings.NewReader(configStream))
}

func TestReadConfigStream11(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || strings.Contains(r.(string), "elecTimeoutRange") == false {
			t.Fatalf("expecting 'elecTimeoutRange', got '%v'", r)
		} else {
			fmt.Printf("  ... Passed\n")
		}
	}()
	fmt.Printf("Test: Invalid elecTimeoutRange 1\n")
	configStream := `raft.self = localhost:12345:0
raft.peers = localhost:12345:0,localhost:12346:1,localhost:12347:2
raft.elecTimeoutRange = 100-(-100)
`

	ParseConfigStream(strings.NewReader(configStream))
}

func TestReadConfigStream12(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || strings.Contains(r.(string), "lower <= upper") == false {
			t.Fatalf("expecting 'lower <= upper', got '%v'", r)
		} else {
			fmt.Printf("  ... Passed\n")
		}
	}()
	fmt.Printf("Test: Invalid elecTimeoutRange 2\n")
	configStream := `raft.self = localhost:12345:0
raft.peers = localhost:12345:0,localhost:12346:1,localhost:12347:2
raft.elecTimeoutRange = 100-50
`

	ParseConfigStream(strings.NewReader(configStream))
}

func TestReadConfigStream13(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || strings.Contains(r.(string), "verbosity") == false {
			t.Fatalf("expecting 'verbosity', got '%v'", r)
		} else {
			fmt.Printf("  ... Passed\n")
		}
	}()
	fmt.Printf("Test: Invalid verbosity\n")
	configStream := `raft.self = localhost:12345:0
raft.peers = localhost:12345:0,localhost:12346:1,localhost:12347:2
raft.verbosity = other
`
	ParseConfigStream(strings.NewReader(configStream))
}

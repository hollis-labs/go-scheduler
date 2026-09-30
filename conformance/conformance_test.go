package conformance_test

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"

	scheduler "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/go-scheduler/conformance"
)

// A correct store must pass the whole suite.
func TestSuitePassesOnCorrectStore(t *testing.T) {
	conformance.Run(t, func(*testing.T) scheduler.Store { return newMemStore() })
}

// TestSuiteInner runs the suite against a deliberately broken store. It is
// skipped unless re-executed by TestSuiteCatchesBrokenStores, because its
// failure is the expected outcome and must not fail the real run.
func TestSuiteInner(t *testing.T) {
	kind := os.Getenv("CONFORMANCE_BROKEN")
	if kind == "" {
		t.Skip("only runs as a subprocess of TestSuiteCatchesBrokenStores")
	}
	conformance.Run(t, func(*testing.T) scheduler.Store {
		m := newMemStore()
		switch kind {
		case "no-expected-fired-at":
			m.dropFiredAt = true
		case "no-claimed-at-fence":
			m.dropClaimFence = true
		case "relabel-expired":
			m.relabelExpired = true
		}
		return m
	})
}

// TestSuiteCatchesBrokenStores is the suite's own proof of value: each store
// with one contract defect must fail the named subtests.
func TestSuiteCatchesBrokenStores(t *testing.T) {
	cases := []struct {
		kind string
		want []string
	}{
		{"no-expected-fired-at", []string{
			"TestSuiteInner/ClaimRejectsStaleExpectedFiredAt",
			"TestSuiteInner/RecoveryRejectsStaleExpectedFiredAt",
			"TestSuiteInner/ClaimRetryingRequiresPriorFiredAt",
		}},
		{"no-claimed-at-fence", []string{"TestSuiteInner/TransitionFencesStaleOwnerAfterRecovery"}},
		{"relabel-expired", []string{"TestSuiteInner/ListDueFiresSelection"}},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestSuiteInner$", "-test.v") //nolint:gosec // re-executes this test binary with fixed arguments
			cmd.Env = append(os.Environ(), "CONFORMANCE_BROKEN="+tc.kind)
			var out bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &out
			if err := cmd.Run(); err == nil {
				t.Fatalf("suite passed a store with defect %q\n%s", tc.kind, out.String())
			}
			for _, name := range tc.want {
				if !strings.Contains(out.String(), "--- FAIL: "+name) {
					t.Errorf("defect %q was not caught by %s\n%s", tc.kind, name, out.String())
				}
			}
		})
	}
}

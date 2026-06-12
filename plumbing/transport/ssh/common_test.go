package ssh

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/transport"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type SuiteCommon struct {
	baseSuite
}

var _ = Suite(&SuiteCommon{})

func (s *SuiteCommon) TestClose(c *C) {
	r := &runner{}
	s.Suite.Start(c, r)

	ep := s.Endpoint(c, "ssh")
	session, err := DefaultClient.NewUploadPackSession(ep, s.Auth)
	c.Assert(err, IsNil)
	c.Assert(session.Close(), IsNil)
}

func (s *SuiteCommon) TestContextCancel(c *C) {
	r := &runner{}
	s.Suite.Start(c, r)

	ep := s.Endpoint(c, "ssh")
	ctx, cancel := context.WithCancel(context.Background())

	session, err := DefaultClient.NewUploadPackSession(ep, s.Auth)
	c.Assert(err, IsNil)
	defer session.Close()

	checkGoroutineLeak(c, func() {
		cancel()

		_, err = session.UploadPack(ctx, &packp.UploadPackRequest{})
		c.Assert(err, Equals, context.Canceled)
	})
}

func checkGoroutineLeak(c *C, fn func()) {
	before := getGoroutines()

	fn()

	// Wait up to 2 seconds for goroutines to exit
	var after []string
	for i := 0; i < 20; i++ {
		after = getGoroutines()
		if len(after) <= len(before) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	// If there are still more goroutines, check if any of them are new
	// and related to ssh/transport
	for _, g := range after {
		if !contains(before, g) && (strings.Contains(g, "ssh") || strings.Contains(g, "transport")) {
			c.Fatalf("goroutine leak detected: %s", g)
		}
	}
}

func getGoroutines() []string {
	buf := make([]byte, 2<<20)
	n := runtime.Stack(buf, true)
	stack := string(buf[:n])
	return strings.Split(stack, "\n\n")
}

func contains(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

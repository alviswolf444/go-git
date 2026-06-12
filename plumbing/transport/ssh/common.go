package ssh

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/internal/common"

	"golang.org/x/crypto/ssh"
)

func init() {
	transport.Register("ssh", DefaultClient)
}

// DefaultClient is the default SSH client.
var DefaultClient = &client{}

// NewClient returns a new SSH client.
func NewClient(config *ssh.ClientConfig) transport.Client {
	return &client{
		clientConfig: config,
	}
}

type client struct {
	clientConfig *ssh.ClientConfig
	*ssh.Client
}

func (c *client) NewUploadPackSession(ep *transport.Endpoint, auth transport.AuthMethod) (transport.UploadPackSession, error) {
	return newSession(c, ep, auth)
}

func (c *client) NewReceivePackSession(ep *transport.Endpoint, auth transport.AuthMethod) (transport.ReceivePackSession, error) {
	return newSession(c, ep, auth)
}

func (c *client) NewCommand(ctx context.Context, ep *transport.Endpoint, auth transport.AuthMethod, cmd string) (common.Command, error) {
	var err error
	var client *ssh.Client
	if auth == nil {
		client, err = c.connect(ep)
	} else {
		client, err = c.connectWithAuth(ep, auth)
	}

	if err != nil {
		return nil, err
	}

	session, err := client.NewSession()
	if err != nil {
		return nil, err
	}

	return &command{
		ctx:     ctx,
		cmd:     cmd,
		session: session,
		client:  client,
	}, nil
}

func (c *client) connect(ep *transport.Endpoint) (*ssh.Client, error) {
	if c.Client != nil {
		return c.Client, nil
	}

	config, err := c.commonConfig(ep)
	if err != nil {
		return nil, err
	}

	config.Auth = append(config.Auth, ssh.RetryableAuthMethod(ssh.PasswordCallback(func() (string, error) {
		return ep.Password, nil
	}), 1))

	return c.dial(ep, config)
}

func (c *client) connectWithAuth(ep *transport.Endpoint, auth transport.AuthMethod) (*ssh.Client, error) {
	if c.Client != nil {
		return c.Client, nil
	}

	a, ok := auth.(AuthMethod)
	if !ok {
		return nil, transport.ErrInvalidAuthMethod
	}

	config, err := a.ClientConfig()
	if err != nil {
		return nil, err
	}

	config.HostKeyCallback, err = c.hostKeyCallback(ep)
	if err != nil {
		return nil, err
	}

	overrideUsername(ep, config)
	return c.dial(ep, config)
}

func (c *client) dial(ep *transport.Endpoint, config *ssh.ClientConfig) (*ssh.Client, error) {
	var port int
	if ep.Port != 0 {
		port = ep.Port
	} else {
		port = 22
	}

	return ssh.Dial("tcp", net.JoinHostPort(ep.Host, strconv.Itoa(port)), config)
}

func (c *client) commonConfig(ep *transport.Endpoint) (*ssh.ClientConfig, error) {
	config := &ssh.ClientConfig{}
	if c.clientConfig != nil {
		*config = *c.clientConfig
	}

	var err error
	config.HostKeyCallback, err = c.hostKeyCallback(ep)
	if err != nil {
		return nil, err
	}

	overrideUsername(ep, config)
	return config, nil
}

func (c *client) hostKeyCallback(ep *transport.Endpoint) (ssh.HostKeyCallback, error) {
	var files []string
	if os.Getenv("SSH_KNOWN_HOSTS") != "" {
		files = strings.Split(os.Getenv("SSH_KNOWN_HOSTS"), string(os.PathListSeparator))
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		files = []string{
			home + "/.ssh/known_hosts",
			home + "/.ssh/known_hosts2",
			"/etc/ssh/ssh_known_hosts",
			"/etc/ssh/ssh_known_hosts2",
		}
	}

	return KnownHostsCallback(files...)
}

func overrideUsername(ep *transport.Endpoint, config *ssh.ClientConfig) {
	if ep.User != "" {
		config.User = ep.User
	}
}

func (c *client) Close() error {
	if c.Client == nil {
		return nil
	}

	return c.Client.Close()
}

// command implements transport.Command
type command struct {
	ctx       context.Context
	cmd       string
	session   *ssh.Session
	client    *ssh.Client
	stdin     io.WriteCloser
	stdout    io.Reader
	stderr    bytes.Buffer
	connected bool
	done      chan struct{}
	closeOnce sync.Once
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (cr *contextReader) Read(p []byte) (int, error) {
	if err := cr.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := cr.r.Read(p)
	if err != nil && cr.ctx.Err() != nil {
		return n, cr.ctx.Err()
	}
	return n, err
}

type contextWriteCloser struct {
	ctx context.Context
	w   io.WriteCloser
}

func (cw *contextWriteCloser) Write(p []byte) (int, error) {
	if err := cw.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := cw.w.Write(p)
	if err != nil && cw.ctx.Err() != nil {
		return n, cw.ctx.Err()
	}
	return n, err
}

func (cw *contextWriteCloser) Close() error {
	if err := cw.ctx.Err(); err != nil {
		return err
	}
	return cw.w.Close()
}

func (c *command) Start() error {
	if err := c.ctx.Err(); err != nil {
		_ = c.session.Close()
		if c.client != nil {
			_ = c.client.Close()
		}
		return err
	}
	if c.connected {
		return transport.ErrAlreadyConnected
	}

	var err error
	stdin, err := c.session.StdinPipe()
	if err != nil {
		return err
	}
	c.stdin = &contextWriteCloser{ctx: c.ctx, w: stdin}

	stdout, err := c.session.StdoutPipe()
	if err != nil {
		return err
	}
	c.stdout = &contextReader{ctx: c.ctx, r: stdout}

	c.session.Stderr = &c.stderr

	err = c.session.Start(c.cmd)
	if err != nil {
		return err
	}

	c.connected = true
	c.done = make(chan struct{})
	go func() {
		select {
		case <-c.ctx.Done():
			_ = c.session.Close()
			if c.client != nil {
				_ = c.client.Close()
			}
		case <-c.done:
		}
	}()

	return nil
}

func (c *command) StdinPipe() (io.WriteCloser, error) {
	return c.stdin, nil
}

func (c *command) StdoutPipe() (io.Reader, error) {
	return c.stdout, nil
}

func (c *command) Wait() error {
	if !c.connected {
		return transport.ErrStartNotCalled
	}

	err := c.session.Wait()
	c.closeDone()

	if c.ctx.Err() != nil {
		return c.ctx.Err()
	}

	if err != nil {
		return err
	}

	return nil
}

func (c *command) closeDone() {
	c.closeOnce.Do(func() {
		if c.done != nil {
			close(c.done)
		}
	})
}

func (c *command) Close() error {
	if !c.connected {
		return nil
	}

	c.connected = false
	c.closeDone()

	// we close the session, but not the client, the client is closed by the
	// session
	return c.session.Close()
}

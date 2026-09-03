package tesira2mqtt

import (
	"context"
	"fmt"
	"io"
	"net"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// rwc joins a separate reader and writer with a close function into one
// io.ReadWriteCloser.
type rwc struct {
	io.Reader
	io.Writer
	closeFn func() error
}

func (c *rwc) Close() error { return c.closeFn() }

// DialSSH connects to the Tesira device and  sets up an SSH connection with a PTY that tesira likes
// A ReadWriteCloser is returned that can be used to read and write TTP commands.
func DialSSH(ctx context.Context, inputCfg SSHConfig) (io.ReadWriteCloser, error) {
	cfg := &ssh.ClientConfig{
		User: inputCfg.Username,
		Auth: []ssh.AuthMethod{
			ssh.KeyboardInteractive(func(_, _ string, questions []string, _ []bool) ([]string, error) {
				answers := make([]string, len(questions))
				for i := range answers {
					answers[i] = inputCfg.Password
				}
				return answers, nil
			}),
			ssh.Password(inputCfg.Password),
		},
		Timeout: inputCfg.Timeout,
	}

	if inputCfg.KnownHostsFile != nil {
		cb, err := knownhosts.New(*inputCfg.KnownHostsFile)
		if err != nil {
			return nil, fmt.Errorf("load known_hosts %s: %w", *inputCfg.KnownHostsFile, err)
		}
		cfg.HostKeyCallback = cb
	} else {
		cfg.HostKeyCallback = ssh.InsecureIgnoreHostKey()
	}

	dialer := net.Dialer{Timeout: inputCfg.Timeout}
	conn, err := dialer.DialContext(ctx, "tcp", inputCfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", inputCfg.Addr, err)
	}

	sshConn, chans, reqs, err := ssh.NewClientConn(conn, inputCfg.Addr, cfg)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("ssh handshake with %s: %w", inputCfg.Addr, err)
	}
	client := ssh.NewClient(sshConn, chans, reqs)

	session, err := client.NewSession()
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("open ssh session: %w", err)
	}

	stdout, err := session.StdoutPipe()
	if err != nil {
		session.Close()
		client.Close()
		return nil, fmt.Errorf("ssh stdout: %w", err)
	}
	stdin, err := session.StdinPipe()
	if err != nil {
		session.Close()
		client.Close()
		return nil, fmt.Errorf("ssh stdin: %w", err)
	}

	if err := session.RequestPty("vt100", 200, 200, ssh.TerminalModes{
		ssh.ECHO: 0,
	}); err != nil {
		session.Close()
		client.Close()
		return nil, fmt.Errorf("request pty: %w", err)
	}
	if err := session.Shell(); err != nil {
		session.Close()
		client.Close()
		return nil, fmt.Errorf("start shell: %w", err)
	}

	return &rwc{
		Reader: stdout,
		Writer: stdin,
		closeFn: func() error {
			session.Close()
			return client.Close()
		},
	}, nil
}

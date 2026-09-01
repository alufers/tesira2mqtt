package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// SSHConfig describes an SSH connection to a Tesira device.
type SSHConfig struct {
	Host     string
	Port     int
	Username string
	Password string

	// VerifyHostKey enables strict host key checking against KnownHostsFile.
	// Tesira devices are commonly reached on closed control networks where the
	// key is not pinned anywhere, so this defaults to off in config.
	VerifyHostKey  bool
	KnownHostsFile string

	Timeout time.Duration
}

// SSHDialer dials Tesira's TTP service over SSH.
type SSHDialer struct{ cfg SSHConfig }

// NewSSHDialer builds an SSH dialer.
func NewSSHDialer(cfg SSHConfig) *SSHDialer { return &SSHDialer{cfg: cfg} }

func (d *SSHDialer) String() string {
	return fmt.Sprintf("ssh://%s@%s", d.cfg.Username, net.JoinHostPort(d.cfg.Host, fmt.Sprint(d.cfg.Port)))
}

// Dial opens an SSH session and starts an interactive shell on it. Tesira's TTP
// server requires a PTY - without one it answers "stdin not a terminal?!,
// exiting" and hangs up.
func (d *SSHDialer) Dial(ctx context.Context) (io.ReadWriteCloser, error) {
	hostKeyCallback, err := d.hostKeyCallback()
	if err != nil {
		return nil, err
	}

	cfg := &ssh.ClientConfig{
		User: d.cfg.Username,
		// Tesira devices with no password configured offer only
		// keyboard-interactive and accept empty answers, so that method comes
		// first; password auth covers devices that do have one set.
		Auth: []ssh.AuthMethod{
			ssh.KeyboardInteractive(func(_, _ string, questions []string, _ []bool) ([]string, error) {
				answers := make([]string, len(questions))
				for i := range answers {
					answers[i] = d.cfg.Password
				}
				return answers, nil
			}),
			ssh.Password(d.cfg.Password),
		},
		HostKeyCallback: hostKeyCallback,
		Timeout:         d.cfg.Timeout,
	}

	addr := net.JoinHostPort(d.cfg.Host, fmt.Sprint(d.cfg.Port))
	dialer := net.Dialer{Timeout: d.cfg.Timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	sshConn, chans, reqs, err := ssh.NewClientConn(conn, addr, cfg)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("ssh handshake with %s: %w", addr, err)
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
		ssh.ECHO: 0, // The TTP server echoes what it needs to; skip the line discipline's copy.
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

func (d *SSHDialer) hostKeyCallback() (ssh.HostKeyCallback, error) {
	if !d.cfg.VerifyHostKey {
		return ssh.InsecureIgnoreHostKey(), nil
	}
	path := d.cfg.KnownHostsFile
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("locate known_hosts: %w", err)
		}
		path = filepath.Join(home, ".ssh", "known_hosts")
	}
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("expand %q: %w", path, err)
		}
		path = filepath.Join(home, path[2:])
	}
	cb, err := knownhosts.New(path)
	if err != nil {
		return nil, fmt.Errorf("load known_hosts %s: %w", path, err)
	}
	return cb, nil
}

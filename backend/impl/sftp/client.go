package sftp

import (
	"context"
	"fmt"
	"net/url"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type Client struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
}

func NewClient(ctx context.Context, connString string) (*Client, error) {
	var (
		user     string
		password string
	)

	u, err := url.Parse(connString)

	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string %s: %w", connString, err)
	}

	if u.User != nil {
		user = u.User.Username()
		password, _ = u.User.Password()
	}

	var sshConf ssh.ClientConfig
	sshConf.HostKeyCallback = ssh.InsecureIgnoreHostKey()
	sshConf.User = user
	sshConf.Auth = []ssh.AuthMethod{
		ssh.Password(password),
	}

	sshClient, err := ssh.Dial("tcp", u.Host, &sshConf)

	if err != nil {
		return nil, fmt.Errorf("failed to create ssh client: %w", err)
	}

	sftpClient, err := sftp.NewClient(sshClient)

	if err != nil {
		return nil, fmt.Errorf("failed to create sftp client: %w", err)
	}

	return &Client{
		sshClient:  sshClient,
		sftpClient: sftpClient,
	}, nil
}

func (c *Client) Close() error {
	var res *multierror.Error

	if err := c.sftpClient.Close(); err != nil {
		err = multierror.Append(res, err)
	}

	if err := c.sshClient.Close(); err != nil {
		err = multierror.Append(res, err)
	}

	return res.ErrorOrNil()
}

func (c *Client) SFTPClient() *sftp.Client {
	return c.sftpClient
}

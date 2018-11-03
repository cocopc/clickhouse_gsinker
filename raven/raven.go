package raven

import (
	"github.com/cocopc/gcommons/log"
	"github.com/getsentry/raven-go"
)



var (
	RavenClient *raven.Client
)

func InitRavenClient(dsn string,username string, email string) {

	l.Debug("初始化raven client")
	client, err := raven.New(dsn)
	if err != nil {
		l.Error("初始化raven client fail", err)
	}
	user := &raven.User{Username: username, Email: email}
	client.SetUserContext(user)
	RavenClient = client
}


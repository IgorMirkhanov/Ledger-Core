// Package migrations embeds SQL migrations of every service.
package migrations

import (
	"embed"
	"io/fs"
)

//go:embed accounts/*.sql
var accountsFS embed.FS

//go:embed transfers/*.sql
var transfersFS embed.FS

//go:embed notifications/*.sql
var notificationsFS embed.FS

func Accounts() fs.FS      { return sub(accountsFS, "accounts") }
func Transfers() fs.FS     { return sub(transfersFS, "transfers") }
func Notifications() fs.FS { return sub(notificationsFS, "notifications") }

func sub(f embed.FS, dir string) fs.FS {
	s, err := fs.Sub(f, dir)
	if err != nil {
		panic(err)
	}
	return s
}

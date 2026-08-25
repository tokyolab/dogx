package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/bootstrapadmin"
	systemdb "github.com/tokyolab/dogx/apps/system/internal/database"

	"github.com/zeromicro/go-zero/core/conf"
	"golang.org/x/term"
)

var (
	configFile = flag.String("f", "apps/system/rpc/etc/system-rpc.yaml", "RPC config file")
	username   = flag.String("username", "admin", "administrator username")
	nickname   = flag.String("nickname", "Administrator", "administrator nickname")
)

type config struct {
	Postgres systemdb.PostgresConf
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "bootstrap administrator: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var c config
	if err := conf.Load(*configFile, &c, conf.UseEnv()); err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	password, err := readPassword()
	if err != nil {
		return err
	}

	database, sqlDB, err := systemdb.OpenPostgres(c.Postgres)
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	user, err := bootstrapadmin.CreateInitialAdministrator(
		context.Background(),
		database,
		authn.NewArgon2id(),
		bootstrapadmin.Input{
			Username: *username,
			Password: password,
			Nickname: *nickname,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("administrator created: id=%d username=%s\n", user.ID, user.Username)
	return nil
}

func readPassword() (string, error) {
	if password := os.Getenv("DOGX_ADMIN_PASSWORD"); password != "" {
		return password, nil
	}
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return "", errors.New("DOGX_ADMIN_PASSWORD is required when stdin is not a terminal")
	}

	fmt.Fprint(os.Stderr, "Administrator password: ")
	password, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", fmt.Errorf("read administrator password: %w", err)
	}
	if strings.TrimSpace(string(password)) == "" {
		return "", errors.New("administrator password is empty")
	}
	return string(password), nil
}

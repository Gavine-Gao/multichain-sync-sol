package database

import (
	"context"

	"github.com/Gavine-Gao/blockchain-sync-sol/config"
)

func SetupDb() *DB {
	dbConfig := config.NewTestDbConfig()

	newDB, _ := NewDB(context.Background(), *dbConfig)
	return newDB
}

package stat

import (
	"github.com/jmoiron/sqlx"
	"sync"
)

type Stat struct {
	dbh       *sqlx.DB
	bulk_stat stStatMap
}

// Одна подписка по одному каналу
type StatSubscription map[string]int

type stStatMap struct {
	mu   sync.RWMutex
	stat map[int]StatSubscription
}

func New(dbh *sqlx.DB) (stat *Stat, err error) {
	var s = new(Stat)
	s.dbh = dbh
	s.bulk_stat.stat = make(map[int]StatSubscription)

	return s, nil
} // New

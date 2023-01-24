package stat

import (
	"database/sql"
	"log"
)

func (stat *Stat) SaveBulkStat() {
	stat.bulk_stat.mu.Lock()
	defer stat.bulk_stat.mu.Unlock()

	var res sql.Result
	var err error
	var rc int64

	for bulk_id, bulk_map := range stat.bulk_stat.stat {
		for _, key := range []string{"sent", "confirmed", "delivered", "entered", "played", "taken_prize", "leaved", "paid"} {
			if _, ok := bulk_map[key]; !ok {
				bulk_map[key] = 0
			}
		}

		if res, err = stat.dbh.Exec(
			`insert into bulk_stat
				(day, bulk, sent, confirmed, delivered, entered, played, taken_prize, leaved, paid)
				values (current_date, $1, $2, $3, $4, $5, $6, $7, $8, $9)
				on conflict(day, bulk) do update set
				sent = bulk_stat.sent + $2,
				confirmed = bulk_stat.confirmed + $3,
				delivered = bulk_stat.delivered + $4,
				entered = bulk_stat.entered + $5,
				played = bulk_stat.played + $6,
				taken_prize = bulk_stat.taken_prize + $7,
				leaved = bulk_stat.leaved + $8,
				paid = bulk_stat.paid + $9`,
			bulk_id,
			bulk_map["sent"],
			bulk_map["confirmed"],
			bulk_map["delivered"],
			bulk_map["entered"],
			bulk_map["played"],
			bulk_map["taken_prize"],
			bulk_map["leaved"],
			bulk_map["paid"],
		); err != nil {
			log.Printf("Stat SaveBulkStat: bulk %d: SQL error: %s, stat lost", bulk_id, err)
			continue
		}
		if rc, err = res.RowsAffected(); err != nil {
			log.Printf("Stat SaveBulkStat: bulk %d: SQL error: %s, stat lost", bulk_id, err)
			continue
		}

		log.Printf("Stat SaveBulkStat: bulk %d values %+v: updated/inserted %d records", bulk_id, bulk_map, rc)
	}

	stat.bulk_stat.stat = make(map[int]StatSubscription)

} // SaveBulkStat

func (stat *Stat) BulkStat(bulk_id int, values map[string]int) {

	stat.bulk_stat.mu.Lock()
	defer stat.bulk_stat.mu.Unlock()

	//	log.Printf("Stat BulkStat: bulk %d values %+v", bulk_id, values)

	if _, ok := stat.bulk_stat.stat[bulk_id]; !ok {
		//		stat.bulk_stat.stat[bulk_id] = make(map[string]int)
		stat.bulk_stat.stat[bulk_id] = StatSubscription{}
	}

	for key, val := range values {
		if _, ok := stat.bulk_stat.stat[bulk_id][key]; !ok {
			stat.bulk_stat.stat[bulk_id][key] = 0
		}
		stat.bulk_stat.stat[bulk_id][key] += val
	}

} // BulkStat

// package bulk provides functions for bulk processing:
// create new bulks, send bulk message, filter msisdns in bulk list and so on
package bulk

import (
	"bufio"
	"bulker/internal/config"
	u "bulker/internal/utility"
	"database/sql"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"log"
	"os"
	"regexp"
)

type Bulk struct {
	dbh  *sqlx.DB
	conf config.BulkConfig
}

// New - create new Bulk object
func New(common *config.Common) (bulk *Bulk, err error) {

	if _, ok := common.Obj["dbh"]; !ok {
		log.Printf("Bulk New: nil Database object")
		return nil, u.E_INVALID_REQ
	}
	bulk = &Bulk{
		dbh:  common.Obj["dbh"].(*sqlx.DB),
		conf: common.Config.Bulk,
	}

	log.Printf("Bulk New: started with config %+v", bulk.conf)

	return bulk, nil

} // New

// ProcessNewBulk - select new bulks, prepare them for sending
func (bulk *Bulk) ProcessNewBulk() {

	var rows *sqlx.Rows
	var err error

	if rows, err = bulk.dbh.Queryx(
		`select 
			bulk_id as bulk_id, coalesce(file, '') as file
        from bulk
        where 
        	active and
            status = 'new'`,
	); err != nil {
		log.Printf("Bulk ProcessNewBulk: SQL error: %s", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var br = new(BulkRecord)
		if err = rows.StructScan(br); err != nil {
			log.Printf("Bulk ProcessNewBulk: StructScan error: %s", err)
			continue
		}

		bulk.processNewBulk(br)

	}

} // ProcessNewBulk

// processNewBulk - prepare new bulk for sending
func (bulk *Bulk) processNewBulk(br *BulkRecord) {

	var err error
	log.Printf("Bulk processNewBulk: %d started", br.Bulk_id)

	if br.File == "" {
		log.Printf("Bulk processNewBulk: %d: empty file field", br.Bulk_id)
		bulk.bulkStatus(br.Bulk_id, "failed")
		return
	}

	re := regexp.MustCompile(`(^.*\/|\?.*$)`)
	br.File = re.ReplaceAllString(br.File, "")

	var fpath string = bulk.conf.File_path + br.File
	var fh *os.File
	if fh, err = os.Open(fpath); err != nil {
		log.Printf("Bulk processNewBulk: %d error open file %s: %s", br.Bulk_id, fpath, err)
		bulk.bulkStatus(br.Bulk_id, "failed")
		return
	}
	defer fh.Close()

	var seen = make(map[string]int)
	var msisdn = make([]int, 0)

	var row_number int = 0

	scanner := bufio.NewScanner(fh)
	for scanner.Scan() {
		var line string = scanner.Text()

		row_number++

		if len(line) == bulk.conf.Msisdn_length-1 {
			line = "7" + line
		}

		var ok bool = bulk.checkMsisdnRegexp(line)
		if !ok {
			log.Printf("Bulk processNewBulk: %d row %d: incorrect msisdn %s, skipping", br.Bulk_id, row_number, line)
			continue
		}

		if _, ok = seen[line]; ok {
			log.Printf("Bulk processNewBulk: %d row %d: already seen msisdn %s", br.Bulk_id, row_number, line)
			continue
		}

		seen[line] = 1

		msisdn = append(msisdn, u.IntVal(line))
	}

	if len(msisdn) == 0 {
		log.Printf("Bulk processNewBulk: %d: empty bulk", br.Bulk_id)
		bulk.bulkStatus(br.Bulk_id, "failed")
		return
	}

	var tx *sqlx.Tx
	var stmt *sqlx.Stmt

	if tx, err = bulk.dbh.Beginx(); err != nil {
		log.Printf("Bulk processNewBulk: %d: begin transaction error: %s", br.Bulk_id, err)
		bulk.bulkStatus(br.Bulk_id, "failed")
		return
	}

	if stmt, err = tx.Preparex(pq.CopyIn("bulk_msisdn", "bulk", "msisdn")); err != nil {
		log.Printf("Bulk processNewBulk: %d: prepare copy statement error: %s", br.Bulk_id, err)
		tx.Rollback()
		bulk.bulkStatus(br.Bulk_id, "failed")
		return
	}
	for _, m := range msisdn {
		_, err = stmt.Exec(br.Bulk_id, m)
		if err != nil {
			log.Printf("Bulk processNewBulk: %d: exec error: %s", br.Bulk_id, err)
			tx.Rollback()
			bulk.bulkStatus(br.Bulk_id, "failed")
			return
		}
	}
	if err = stmt.Close(); err != nil {
		log.Printf("Bulk processNewBulk: %d: close error: %s", br.Bulk_id, err)
		tx.Rollback()
		bulk.bulkStatus(br.Bulk_id, "failed")
		return
	}

	tx.Commit()
	bulk.bulkStatus(br.Bulk_id, "sending")

} // processNewBulk

// bulkStatus - update bulk status in table bulk
func (bulk *Bulk) bulkStatus(id int, status string) {

	var rc int64
	res, err := bulk.dbh.Exec(
		`update bulk
       	set 
			status = $1,
			finish_stamp = case when $1::bulk_status_type = 'finished' then now() else finish_stamp end
        where bulk_id = $2`,
		status, id,
	)
	if err != nil {
		log.Printf("Bulk bulkStatus: %d %s: SQL error: %s", id, status, err)
		return
	}
	if rc, err = res.RowsAffected(); err != nil {
		log.Printf("Bulk bulkStatus: %d %s: SQL error: %s", id, status, err)
		return
	}

	log.Printf("Bulk bulkStatus: %d %s updated %d records", id, status, rc)

} // bulkStatus

// checkMsisdnRegexp - check if msisdn is correct
func (bulk *Bulk) checkMsisdnRegexp(msisdn string) (ok bool) {

	var re *regexp.Regexp
	re = regexp.MustCompile(bulk.conf.Msisdn_regexp)

	if re.Match([]byte(msisdn)) {
		return true
	}

	return false

} // checkMsisdnRegexp

// BulkDone - check if all msisdns in bulk list are processed. If so, set bulk status to finished
func (bulk *Bulk) BulkDone(id int) {

	row := bulk.dbh.QueryRowx(
		`select count(*)
       	from bulk_msisdn
        where
       		bulk = $1 and
            send_sms and
            not sent`, id,
	)
	var err error
	var n int

	if err = row.Scan(&n); err != nil {
		log.Printf("Bulk BulkDone: %d: SQL error %s", id, err)
		return
	}

	if n == 0 {
		bulk.bulkStatus(id, "finished")
	}

} // BulkDone

// LoadBulkList - get list of active sending bulks
func (bulk *Bulk) LoadBulkList() (list []BulkRecord, err error) {

	list = make([]BulkRecord, 0)

	var rows *sqlx.Rows
	if rows, err = bulk.dbh.Queryx(
		`select
			bulk_id, bulk_type, 
			subscription, subscription_filter,
			message, 
			to_char(start_time, 'HH24:MI:SS') as start_time, 
			to_char(end_time, 'HH24:MI:SS') as end_time,
			load_limit, daily_limit
		from bulk
		where
			active and
			start_stamp <= current_date + current_time and
			status = 'sending' and
			message is not null`,
	); err != nil && err != sql.ErrNoRows {
		log.Printf("Bulk LoadBulkList: SQL error: %s", err)
		return nil, u.E_INTERNAL
	}
	defer rows.Close()

	for rows.Next() {
		var br BulkRecord
		if err = rows.StructScan(&br); err != nil {
			log.Printf("Bulk LoadBulkList: StructScan error: %s", err)
			continue
		}
		//		log.Printf("Bulk LoadBulkLst: got bulk record: bulk_id %d type %s filter %s subscription %d", br.Bulk_id, br.Bulk_type, br.Subscription_filter, br.Subscription)
		list = append(list, br)
	}

	return list, nil

} // LoadBulkList

// BulkMsisdnList - get list of not processed yet msisdns for particelar bulk
func (bulk *Bulk) BulkMsisdnList(br BulkRecord) (list []*VisitorRecord, err error) {

	list = make([]*VisitorRecord, 0)

	var limit int = bulk.loadLimit(br)
	var got int = 0

	//	for to_load := limit; to_load > 0; to_load = limit - len(list) {
	for to_load := limit; to_load > 0; to_load = limit - got {
		var l []*VisitorRecord
		var to_send int

		// Ничего не получили, заканчиваем загрузку
		if l, to_send, _, err = bulk.getBulkMsisdn(br, to_load, list); err != nil || len(l) == 0 {
			break
		}
		list = append(list, l...)
		got += to_send
	}

	if len(list) > 0 {
		log.Printf("Bulk BulkMsisdnList: bulk %d type %s subscription %d filter %s: done with %d records", br.Bulk_id, br.Bulk_type, br.Subscription, br.Subscription_filter, len(list))
	}

	return list, nil

} // BulkMsisdnList

// getBulkMsisdn - get list of not processed msisdns for particelar bulk, call filtration on that list
func (bulk *Bulk) getBulkMsisdn(br BulkRecord, limit int, exclude_list []*VisitorRecord) (list []*VisitorRecord, to_send, disabled int, err error) {

	var exclude = []int{0}

	if exclude_list == nil {
		exclude_list = make([]*VisitorRecord, 0)
	}

	for _, vr := range exclude_list {
		exclude = append(exclude, vr.Msisdn)
	}

	list = make([]*VisitorRecord, 0)

	var rows *sqlx.Rows
	if rows, err = bulk.dbh.Queryx(
		`select 
			bm.msisdn, to_char(current_date, 'DD.MM.YYYY') as date,
			coalesce(s.subscription_id, 0) as subscription,
			bm.send_sms
		from bulk_msisdn bm
		left join visitor v on bm.msisdn = v.msisdn
		left join subscription s on coalesce(v.game_level,0) = s.game_level
        left join def_code dc on dc.min <= bm.msisdn and dc.max >= bm.msisdn
        left join region r on dc.region = r.code 
        left join timezone t on r.timezone = t.timezone_id
		where
			bm.bulk = $1 and
			bm.send_sms and
			not bm.sent and
			bm.msisdn != all($3::bigint[]) and
			current_time + (coalesce(t.msk_offset, 0) || 'hours')::interval between $4::time and $5::time
		limit $2`, br.Bulk_id, limit, pq.Array(exclude), br.Start_time, br.End_time,
	); err != nil && err != sql.ErrNoRows {
		log.Printf("Bulk getBulkMsisdn: bulk %d: SQL error: %s", br.Bulk_id, err)
		bulk.bulkStatus(br.Bulk_id, "failed")
		return nil, 0, 0, u.E_INTERNAL
	}
	defer rows.Close()

	for rows.Next() {
		var vr = new(VisitorRecord)
		if err = rows.StructScan(&vr); err != nil {
			log.Printf("Bulk getBulkMsisdn: bulk %d subscription %d limit %d: StructScan error: %s", br.Bulk_id, br.Subscription, limit, err)
			continue
		}
		list = append(list, vr)
	}

	if len(list) > 0 {
		log.Printf("Bulk getBulkMsisdn: bulk %d susbcription %d limit %d: got %d records", br.Bulk_id, br.Subscription, limit, len(list))
	}

	if to_send, disabled, err = bulk.filterMsisdnList(br, list); err != nil {
		return nil, 0, 0, u.E_INTERNAL
	}

	return list, to_send, disabled, nil
} // getBulkMsisdn

// loadLimit - get limit for bulk msisdn load
func (bulk *Bulk) loadLimit(br BulkRecord) (limit int) {

	// Проверяем на достижения лимита в день
	limit = br.Load_limit
	var amount int

	if br.Daily_limit > 0 {
		amount = bulk.sentToday(br.Bulk_id)
		switch {
		// превысили лимит, не шлем
		case amount >= br.Daily_limit:
			limit = 0
		// Еще можно отправить, но меньше чем Load_limit
		case br.Daily_limit-amount < limit:
			limit = br.Daily_limit - amount
		default:
		}
	}

	//	log.Printf("Bulk loadLimit: bulk %d daily_limit %d sent %d limit %d", br.Bulk_id, br.Daily_limit, amount, limit)

	return limit

} // loadLimit

// sentToday - get amount of sms sent today for particular bulk id
func (bulk *Bulk) sentToday(id int) (amount int) {

	var row *sqlx.Row
	var err error

	row = bulk.dbh.QueryRowx(
		`select coalesce(sum(sent),0) from bulk_stat
        where 
        	day = current_date and
            bulk = $1`, id,
	)
	if err = row.Scan(&amount); err != nil {
		log.Printf("Bulk sentToday: id %d: Scan error: %s", id, err)
		return 1000000
	}

	return amount
} // sentToday

// filterMsisdnList - filter list of msisdns by different parameters
func (bulk *Bulk) filterMsisdnList(br BulkRecord, list []*VisitorRecord) (to_send, disabled int, err error) {

	if list == nil {
		return 0, 0, u.E_INVALID_REQ
	}

	// Нечего фильтровать
	if len(list) == 0 {
		return 0, 0, nil
	}

	var seen = make(map[int]int)
	var disable = make([]int, 0)
	var send = make([]int, 0)

	for _, vr := range list {
		// Паранойя
		if _, ok := seen[vr.Msisdn]; ok {
			log.Printf("Bulk filterMsisdnList: bulk %d already seen msisdn %d, skipping", br.Bulk_id, vr.Msisdn)
			vr.Send_sms = false
			disable = append(disable, vr.Msisdn)
			continue
		}
		seen[vr.Msisdn] = 1

		switch br.Subscription_filter {
		// Если тип балка all - нечего фильтровать
		case "all":
			send = append(send, vr.Msisdn)
		// Шлем только подписантам
		case "subscribed":
			switch {
			// Этот вообще не подписан, не шлем
			case vr.Subscription == 0:
				vr.Send_sms = false
				disable = append(disable, vr.Msisdn)
			// Задана конкретная подписка, не совпадает с подпиской абонента
			case br.Subscription > 0 && vr.Subscription != br.Subscription:
				vr.Send_sms = false
				disable = append(disable, vr.Msisdn)
			default:
				send = append(send, vr.Msisdn)
			}
		// Шлем только неподписанным
		case "not_subscribed":
			if vr.Subscription > 0 {
				vr.Send_sms = false
				disable = append(disable, vr.Msisdn)
			} else {
				send = append(send, vr.Msisdn)
			}
		}
		log.Printf("Bulk filterMsisdnList: bulk %d filter %s: msisdn %d subscription %d send_sms %t", br.Bulk_id, br.Subscription_filter, vr.Msisdn, vr.Subscription, vr.Send_sms)
	}

	log.Printf("Bulk filterMsisdnList: bulk %d filter %s: got %d records to disable and %d records to send", br.Bulk_id, br.Subscription_filter, len(disable), len(send))

	if _, err = bulk.setSmsOption("send_sms", false, br.Bulk_id, disable); err != nil {
		return 0, 0, u.E_INTERNAL
	}
	if _, err = bulk.setSmsOption("sent", true, br.Bulk_id, send); err != nil {
		return 0, 0, u.E_INTERNAL
	}

	return len(send), len(disable), nil

} // filterMsisdnList

// setSmsOption - set sms flags in table bulk_msisdn for list of msisdns
func (bulk *Bulk) setSmsOption(field string, option bool, bulk_id int, list []int) (rc int64, err error) {

	if list == nil {
		return 0, u.E_INVALID_REQ
	}
	if len(list) == 0 {
		return 0, nil
	}

	var res sql.Result

	if res, err = bulk.dbh.Exec(
		`update bulk_msisdn
		set `+field+` = $1,
		sent_stamp = now()
		where
			bulk = $2 and
			msisdn = any($3::bigint[])`,
		option, bulk_id, pq.Array(list),
	); err != nil {
		log.Printf("Bulk setSmsOption: %s %t bulk %d: SQL error: %s", field, option, bulk_id, err)
		return 0, u.E_INTERNAL
	}
	if rc, err = res.RowsAffected(); err != nil {
		log.Printf("Bulk setSmsOption: %s %t bulk %d: SQL error: %s", field, option, bulk_id, err)
		return 0, u.E_INTERNAL
	}

	log.Printf("Bulk setSmsOption: %s %t bulk %d: updated %d records", field, option, bulk_id, rc)
	return rc, nil

} // setSmsOption

// VisitorReact - set visitor reaction on bulk message
func (bulk *Bulk) VisitorReact(vr *VisitorReact) (err error) {

	var row *sqlx.Row
	row = bulk.dbh.QueryRowx(
		`with dd as (
			select b.bulk_id from bulk_msisdn bm
			join bulk b on bm.bulk = b.bulk_id
			where
				bm.msisdn = $1 and
				bm.sent_stamp >= current_date - ($3 || ' days')::interval and
				not `+vr.Action+` and
				bm.sent and
				b.bulk_type = $2::visitor_reaction_type
			order by b.bulk_id desc
			limit 1
		)
		update bulk_msisdn set `+vr.Action+` = true
		where
			msisdn = $1 and
			bulk = (select bulk_id from dd)
		returning bulk`, vr.Msisdn, vr.Bulk_type, vr.Days,
	)
	if err = row.Scan(&vr.Bulk_id); err != nil && err != sql.ErrNoRows {
		log.Printf("Bulk VisitorReact: msisdn %d bulk_type %s action %s days %d: Scan error: %s", vr.Msisdn, vr.Bulk_type, vr.Action, vr.Days, err)
		return u.E_INTERNAL
	}

	return nil

} // VisitorReact

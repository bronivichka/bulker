// package bulker provides bulk sms sending functional
package bulker

import (
	"bulker/internal/bulk"
	"bulker/internal/config"
	"bulker/internal/logmanager"
	"bulker/internal/rabbitmq"
	"bulker/internal/sms"
	"bulker/internal/stat"
	u "bulker/internal/utility"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Bulker struct {
	conf       config.BulkConfig
	common     *config.Common
	dbh        *sqlx.DB
	rmq        *rabbitmq.RabbitMQ
	logfile    *os.File
	sms        *sms.Sms
	stat       *stat.Stat
	bulk       *bulk.Bulk
	wgmanager  sync.WaitGroup
	wgworker   sync.WaitGroup
	wgconsume  sync.WaitGroup
	csend      chan map[string]interface{}
	pchan      chan bool
	ptimer     *time.Timer
	in_process bool
}

// Run run bulker process
func (bulker *Bulker) Run() {

	defer bulker.logfile.Close()
	defer os.Remove(bulker.conf.File.PidFile)

	go bulker.workerManager()
	bulker.wgmanager.Add(1)
	log.Printf("bulker Run: waiting for workerManager to finish")
	bulker.wgmanager.Wait()

	log.Printf("bulker Run: terminated")

} // Run

// New create an object of bulker type
func New(cfile string) (bulker *Bulker, err error) {

	if cfile == "" {
		return nil, u.E_INVALID_REQ
	}

	bulker = new(Bulker)
	bulker.common = new(config.Common)
	bulker.common.Obj = make(map[string]interface{})

	// Сначала читаем конфиг и раскладываем в нужные поля
	if err = config.ReadConfig(cfile, &bulker.common.Config, []config.Alias{{Alias: "Database", Key: "DB"}, {Alias: "RabbitMQ", Key: "RabbitMQConfig"}}...); err != nil {
		log.Printf("bulker New: unable to read config: %s", err)
		return nil, err
	}
	bulker.conf = bulker.common.Config.Bulk
	log.Printf("bulker New: got config %+v", bulker.conf)

	// Открываем логфайл
	if bulker.logfile, err = logmanager.OpenLog(bulker.conf.File.LogFile); err != nil {
		return nil, err
	}

	// PID файл
	if err = u.SavePID(bulker.conf.File.PidFile); err != nil {
		return nil, err
	}

	// БД
	if err = bulker.connectToDB(bulker.common.Config.DB); err != nil {
		log.Printf("bulker New: unable to connect to database: %v", err)
		return nil, err
	}
	bulker.common.Obj["dbh"] = bulker.dbh

	// Кролик
	if bulker.rmq, err = rabbitmq.ConnectToRabbitMQ(bulker.common.Config.RabbitMQ); err != nil {
		log.Printf("bulker New: unable to connect to RabbitMQ: %s", err)
		return nil, err
	}
	bulker.common.Obj["rmq"] = bulker.rmq

	bulker.csend = make(chan map[string]interface{})
	bulker.common.Csend = bulker.csend

	bulker.pchan = make(chan bool)

	// Stat
	if bulker.stat, err = stat.New(bulker.dbh); err != nil {
		log.Printf("bulker New: unable to create Stat object: %s", err)
		return nil, err
	}
	bulker.common.Obj["stat"] = bulker.stat

	// SMS
	if bulker.sms, err = sms.New(bulker.common); err != nil {
		log.Printf("bulker New: unable to create SMS object: $s", err)
		return nil, err
	}
	bulker.common.Obj["sms"] = bulker.sms

	// Bulk
	if bulker.bulk, err = bulk.New(bulker.common); err != nil {
		log.Printf("bulker New: unable to create Bulk object: %s", err)
		return nil, err
	}
	bulker.common.Obj["bulk"] = bulker.bulk

	// Сигналы
	sigChan := make(chan os.Signal, 1)
	signal.Notify(
		sigChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	go bulker.handleSignal(sigChan)

	return bulker, nil

} // New

// handleSignal handle system signals
func (bulker *Bulker) handleSignal(c chan os.Signal) {
	for {
		sig := <-c
		var sigstr = sig.String()
		log.Printf("bulker handleSignal: got signal %+v sigstr %+v", sig, sigstr)
		if sig.String() == "hangup" {
			if status := logmanager.RotateLog(bulker.logfile, bulker.conf.File.LogFile); !status {
				log.Printf("bulker handleSignal: error occured while rotating log %s", bulker.conf.File.LogFile)
				bulker.finish()
			}
		} else {
			bulker.finish()
		}
	}
} // handleSignal

// connectToDB connect to database
func (bulker *Bulker) connectToDB(dbconf config.DBConfig) (err error) {
	dbinfo := "user=" + dbconf.User + " password=" + dbconf.Password + " dbname=" + dbconf.Database + " host=" + dbconf.Host + " sslmode=disable"

	bulker.dbh, err = sqlx.Open("postgres", dbinfo)

	if err == nil && bulker.conf.Max_idle_conn > 0 {
		bulker.dbh.SetMaxIdleConns(bulker.conf.Max_idle_conn)
	}

	return err
} // connectToDB

// worketManager start different workers
// periodicWorker for do periodical work
// senderWorker for send message to RabbitMQ
// consume to RabbitMQ queue for reading currency events
func (bulker *Bulker) workerManager() {
	log.Printf("bulker workerManager starting")

	// отправка сообщений в кролика
	go bulker.sms.SenderWorker()

	// периодические задачи
	go bulker.periodicWorker()
	bulker.wgworker.Add(1)

	// Подписываемся на очередь в Кролике
	go bulker.consume(bulker.conf.Consume_queue, bulker.conf.Consume_queue, bulker.handleSMPPEvent)
	bulker.wgconsume.Add(1)

	bulker.wgconsume.Wait()
	bulker.wgworker.Wait()
	log.Printf("bulker workerManager done")

	bulker.wgmanager.Done()
} // workerManager

// periodicWorker do periodic work:
// process bulks (create, send, finish)
// save bulk stats
func (bulker *Bulker) periodicWorker() {
	log.Printf("bulker periodicWorker starting")

	// раз в Х секунд
	bulker.ptimer = time.NewTimer(time.Second * time.Duration(bulker.conf.Interval))
FOR:
	for {
		select {
		case <-bulker.ptimer.C:
			bulker.processBulk()
			bulker.stat.SaveBulkStat()
			bulker.ptimer = time.NewTimer(time.Second * time.Duration(bulker.conf.Interval))
		case <-bulker.pchan:
			bulker.ptimer.Stop()
			break FOR
		}
	}
	bulker.wgworker.Done()

	log.Printf("bulker periodicWorker done")
} // periodicWorker

// finish finishes bulker process
func (bulker *Bulker) finish() {

	log.Printf("bulker finish: stopping periodicWorker")
	bulker.pchan <- true

	log.Printf("bulker finish: saving bulker statistics")
	bulker.stat.SaveBulkStat()

	log.Printf("bulker finish: closing csend")
	close(bulker.csend)

	log.Printf("bulker finish: disconnecting from RabbitMQ")
	bulker.rmq.Finish()

} // finish

// processBulk provides main bulker cycle
// create new bulks if any
// send bulk messages
func (bulker *Bulker) processBulk() {

	if bulker.in_process {
		log.Printf("bulker processBulk: already in process, exiting")
		return
	}

	bulker.inProcessSet(true)
	defer bulker.inProcessSet(false)

	// Новые балки - обрабатываем
	bulker.bulk.ProcessNewBulk()

	// Рассылка балков
	bulker.sendBulk()

} // processBulk

// inProcessSet sets flag in_process
// if flag set, new periodic iteration will not start
func (bulker *Bulker) inProcessSet(val bool) {
	bulker.in_process = val
}

// sendBulkMessage sends bulk message to abonent
// message goes into RabbitMQ queue
func (bulker *Bulker) sendBulkMessage(bulk bulk.BulkRecord, vr bulk.VisitorRecord) {

	msg := make(map[string]interface{})
	msg["msisdn"] = vr.Msisdn
	msg["to"] = vr.Msisdn
	msg["message"] = bulk.Message
	msg["date"] = vr.Date
	msg["source_route"] = "bulker"
	msg["bulk_id"] = bulk.Bulk_id
	msg["subscription"] = bulk.Subscription

	bulker.csend <- msg

} // sendBulkMessage

// consume consumes to RabbitMQ queue for reading currency accrue events from another services
func (bulker *Bulker) consume(qname, cname string, fn func(msg *rabbitmq.RabbitMQMessage)) {

	log.Printf("bulker consume: to queue %s consumer %s start", qname, cname)
	bulker.rmq.Consume(qname, cname, fn)
	log.Printf("bulker consume: to queue %s consumer %s done", qname, cname)
	bulker.wgconsume.Done()

} // consume

// handleEvent handles event from RabbitMQ queue
// those events are requests for currency accruing
func (bulker *Bulker) handleSMPPEvent(msg *rabbitmq.RabbitMQMessage) {

	log.Printf("bulker handleSMPPEvent: got message routing key %s headers %+v body %s", msg.RoutingKey, msg.Headers, string(msg.Body))

	// Шлем Ack сразу
	var sendmsg = make(map[string]interface{})
	sendmsg["delivery_tag"] = msg.DeliveryTag
	bulker.csend <- sendmsg

	// Обрабатываем
	if msg.RoutingKey != "delivery_status_bulker" {
		return
	}

	// Статус
	var status string
	if _, ok := msg.Headers["delivery_status"]; ok {
		status = u.StrVal(msg.Headers["delivery_status"])
	}
	if status != "DELIVRD" {
		return
	}

	var bulk_id int
	if _, ok := msg.Headers["bulk_id"]; !ok {
		return
	}
	bulk_id = u.IntVal(msg.Headers["bulk_id"])

	bulker.stat.BulkStat(bulk_id, map[string]int{"delivered": 1})

} // handleSMPPEvent

// sendBulk
// load bulk list for sending
// load msisdn list for everu bulk
// send bulk messages to loaded abonents
// mark bulk as finished if necessary
func (bulker *Bulker) sendBulk() {

	var bulk_list []bulk.BulkRecord
	var err error
	// список балков к рассылке
	if bulk_list, err = bulker.bulk.LoadBulkList(); err != nil {
		return
	}

	for _, br := range bulk_list {
		// список абонентов к рассылке
		// фильтрация абонентов там же сразу
		var msisdn_list []*bulk.VisitorRecord
		if msisdn_list, err = bulker.bulk.BulkMsisdnList(br); err != nil {
			continue
		}

		for _, vr := range msisdn_list {
			bulker.sendMessage(br, vr)
		}

		bulker.bulk.BulkDone(br.Bulk_id)
	}

} // sendBulk

// sendMessage
// send message to abonent
// it sends message to RabbitMQ queue
func (bulker *Bulker) sendMessage(br bulk.BulkRecord, vr *bulk.VisitorRecord) {

	if !vr.Send_sms {
		return
	}

	log.Printf("bulker sendMessage: bulk %d msisdn %d send_sms %t message %s", br.Bulk_id, vr.Msisdn, vr.Send_sms, br.Message)
	bulker.stat.BulkStat(br.Bulk_id, map[string]int{"sent": 1})

	msg := make(map[string]interface{})
	msg["msisdn"] = vr.Msisdn
	msg["to"] = vr.Msisdn
	msg["message"] = br.Message
	msg["date"] = vr.Date
	msg["source_route"] = "bulker"
	msg["bulk_id"] = br.Bulk_id
	msg["subscription"] = br.Subscription

	bulker.csend <- msg

} // sendMessage

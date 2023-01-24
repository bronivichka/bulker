// package sms sends SMS to abonents via RabbitMQ
// SMS message text can be taken from the database by type
package sms

import (
	"bulker/internal/config"
	"bulker/internal/rabbitmq"
	u "bulker/internal/utility"
	"github.com/jmoiron/sqlx"
	"log"
	"strings"
)

type Sms struct {
	rmq         *rabbitmq.RabbitMQ
	dbh         *sqlx.DB
	Csend       chan map[string]interface{}
	global_conf config.GlobalConfig
}

// New - create Sms object
func New(c *config.Common) (sms *Sms, err error) {

	sms = new(Sms)
	if c == nil {
		log.Printf("SMS New: nil config object!")
		return nil, u.E_INVALID_REQ
	}

	if c.Obj["dbh"] == nil {
		log.Printf("SMS New: nil database object")
		return nil, u.E_INVALID_REQ
	}
	sms.dbh = c.Obj["dbh"].(*sqlx.DB)
	sms.global_conf = c.Config.Global

	if c.Obj["rmq"] == nil {
		log.Printf("SMS New: nil RabbitMQ object")
		return nil, u.E_INVALID_REQ
	}
	sms.rmq = c.Obj["rmq"].(*rabbitmq.RabbitMQ)

	sms.Csend = c.Csend

	return sms, nil
} // New

// SenderWorker - read Csend channel, process messages and send them to RabbitMQ queue
func (sms *Sms) SenderWorker() {

	log.Printf("SMS SenderWorker: ready to send")

	if sms.Csend == nil {
		log.Printf("SMS SenderWorker: nill Csend value, nothing to start")
		return
	}

	for msg := range sms.Csend {
		log.Printf("SMS SenderWorker: got message: %+v", msg)

		// это ack
		if _, ok := msg["delivery_tag"]; ok {
			sms.rmq.Ack(msg["delivery_tag"])
			continue
		}

		// to должен быть определен
		var to string
		if _, ok := msg["to"]; !ok {
			log.Printf("SMS SenderWorker: recipient parameter (to) not set, nothing to do")
			continue
		}
		to = u.StrVal(msg["to"])

		// from
		from := sms.global_conf.Free_number
		if _, ok := msg["from"]; ok {
			from = msg["from"].(string)
		}

		// message
		var msg_text string
		msg_text = sms.BuildMessageText(msg)

		// route
		route := "free"
		if _, ok := msg["route"]; ok {
			route = msg["route"].(string)
		}

		if msg_text == "" {
			log.Printf("SMS SenderWorker: empty message, from %s to %s, skipping", from, to)
			continue
		}

		var rmqmsg = new(rabbitmq.RabbitMQMessage)
		rmqmsg.Headers = make(map[string]interface{})
		rmqmsg.Headers["from"] = from
		rmqmsg.Headers["to"] = to
		rmqmsg.Body = []byte(msg_text)

		for key, val := range msg {
			if _, ok := rmqmsg.Headers[key]; !ok {
				rmqmsg.Headers[key] = u.StrVal(val)
			}
		}

		if err := sms.rmq.Publish(route, rmqmsg); err != nil {
			log.Printf("SMS SenderWorker: error while sending message: %s", err)
		}
	}

	log.Printf("SMS SenderWorker: done")

} // SenderWorker

// BuildMessageText - build message from given map
// search message text by its type if no message found
// make some placeholder replacements
func (sms *Sms) BuildMessageText(msg map[string]interface{}) (message string) {

	var lang string = "ru"

	if _, ok := msg["message"]; ok {
		message = msg["message"].(string)
	} else if _, ok = msg["type"]; ok {
		message = sms.GetSmsTextByType(u.StrVal(msg["type"]), lang)
	}
	if message == "" {
		log.Printf("SMS BuildMessageText: %+v no message found", msg)
		return message
	}

	// Преобразования
	for _, pl := range []string{"servicename", "msisdn", "link", "code", "date", "prize", "place"} {
		var repl string
		if _, ok := msg[pl]; ok {
			repl = u.StrVal(msg[pl])
		}

		message = strings.Replace(message, "%"+pl+"%", repl, -1)
	}

	return message

} // BuildMessageText

// GetSmsTextByType - get sms text message by its type from sms_text table
func (sms *Sms) GetSmsTextByType(t string, l string) (text string) {

	var row *sqlx.Row
	row = sms.dbh.QueryRowx(
		`select 	
			message
		from sms_text 
		where 
			active and 
			type = $1 and
			language = $2
		order by random()
		limit 1`, t, l,
	)
	err := row.Scan(&text)

	log.Printf("SMS GetSmsTextByType: type %s language %s: got text %s err %s", t, l, text, err)
	return text

} // GetSmsTextByType

// package rabbitmq provides functions for work with RabbitMQ broker
package rabbitmq

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"time"
)

// queue creation args
type QueueArgs map[string]interface{}

// queue settings
type Queue struct {
	Name    string
	Binding []string
	Args    QueueArgs
}

// exchange, settings
type Exchange struct {
	Name       string // Имя
	Kind       string // Тип
	Queue      []Queue
	Expiration string
	Priority   uint8
}

// RabbitMQ global config
type RabbitMQConfig struct {
	Host           string
	Port           int
	VirtualHost    string
	Login          string
	Password       string
	PrefetchCount  int
	PrefetchSize   int
	ReconnectDelay int
	ReconsumeDelay int
	Exchange       Exchange // карта exchanges
}

// consumer
type consumer struct {
	queue   string
	consume bool
}

type consumermap map[string]*consumer

type RabbitMQ struct {
	conf       RabbitMQConfig
	channel    *amqp.Channel
	conn       *amqp.Connection
	paused     bool
	connected  bool
	terminated bool
	consumer   consumermap
	cchan      struct {
		notifyClose   chan *amqp.Error
		notifyCancel  chan string
		notifyPublish chan amqp.Confirmation
		notifyFlow    chan bool
		notifyReturn  chan amqp.Return
		stopchannel   chan bool
	}
	Csend chan map[string]interface{}
}

type RabbitMQMessage struct {
	Body        []byte
	Headers     map[string]interface{}
	Expiration  string
	Priority    uint8
	DeliveryTag uint64
	RoutingKey  string
}

var (
	errNotConnected      = errors.New("Not connected")
	errTerminating       = errors.New("Terminating")
	errConsumerNotExists = errors.New("Consumer does not exists")
	errNotConsumed       = errors.New("Consumer not consumed")
)

// New - create RabbitMQ object
func New(rconf RabbitMQConfig) *RabbitMQ {
	var rmq = new(RabbitMQ)
	rmq.conf = rconf
	rmq.cchan.stopchannel = make(chan bool)
	rmq.consumer = make(consumermap)
	rmq.Csend = make(chan map[string]interface{}, 100)
	return rmq
}

// connectToServer - connect to RabbitMQ server
// set connected flag to true if OK
func (rmq *RabbitMQ) connectToServer() (err error) {

	rmq.connected = false

	if rmq.terminated {
		return errTerminating
	}

	// Коннект
	var connstr = "amqp://" + rmq.conf.Login + ":" + rmq.conf.Password + "@" + rmq.conf.Host + ":" + strconv.Itoa(rmq.conf.Port) + "/" + rmq.conf.VirtualHost
	log.Printf("RabbitMQ connectToServer: connecting to uri %s", connstr)
	rmq.conn, err = amqp.Dial(connstr)
	if err != nil {
		return err
	}

	// Канал
	if rmq.channel, err = rmq.conn.Channel(); err != nil {
		rmq.handleFatalError()
		return err
	}
	//	channel.Confirm(true)
	rmq.connected = true
	rmq.paused = false

	rmq.cchan.notifyPublish = make(chan amqp.Confirmation)
	rmq.cchan.notifyClose = make(chan *amqp.Error)
	rmq.cchan.notifyCancel = make(chan string)
	rmq.cchan.notifyFlow = make(chan bool)
	rmq.cchan.notifyReturn = make(chan amqp.Return)

	rmq.channel.NotifyPublish(rmq.cchan.notifyPublish)
	rmq.channel.NotifyClose(rmq.cchan.notifyClose)
	rmq.channel.NotifyCancel(rmq.cchan.notifyCancel)
	rmq.channel.NotifyFlow(rmq.cchan.notifyFlow)
	rmq.channel.NotifyReturn(rmq.cchan.notifyReturn)

	rmq.channel.Qos(rmq.conf.PrefetchCount, rmq.conf.PrefetchSize, true)

	return nil

} // connectToServer

// Connect - connect to RabbitMQ server
func (rmq *RabbitMQ) Connect() (err error) {

	if err = rmq.connectToServer(); err != nil {
		return err
	}

	go rmq.handleConnection()

	return nil
} // Connect

// handleConnection - handle connection with RabbitMQ server
// reconnect if disconnected
func (rmq *RabbitMQ) handleConnection() (err error) {

	log.Printf("RabbitMQ handleConnection: started")

FLABEL:
	for {

		switch rmq.connected {
		case true:
			// handleChannel - блокирующая функция, если она вышла, значит, либо ошибка канала, либо прислали сигнал в канал stopchannel
			if err = rmq.handleChannel(); err != nil {
				log.Printf("RabbitMQ handleConnection: got error from handleChannel: %v", err)
			}
		case false:
			err = rmq.connectToServer()
			if err == nil {
				continue FLABEL
			}
			log.Printf("RabbitMQ handleConnection: got error from connectToServer: %v", err)
		}

		if rmq.terminated {
			break
		}

		<-time.NewTimer(time.Duration(rmq.conf.ReconnectDelay) * time.Second).C
	}

	// Отконнектимся
	if rmq.connected {
		if err = rmq.conn.Close(); err != nil {
			log.Printf("RabbitMQ handleConnection: got error from connection Close: %v", err)
		}
	}

	log.Printf("RabbitMQ handleConnection: done, returning %v", err)

	return err
} // handleConnection

// handleChannel - handle channel events
func (rmq *RabbitMQ) handleChannel() (err error) {
	if err := rmq.channel.Confirm(false); err != nil {
		log.Printf("RabbitMQ handleChannel: got err %+v while attempting to make channel confirm", err)
		return err
	}

	for {
		select {
		case v := <-rmq.cchan.notifyPublish:
			rmq.notifyPublishEvent(v)
		case v := <-rmq.cchan.notifyClose:
			rmq.connected = false
			log.Printf("RabbitMQ handleChannel: got notifyClose event %v", v)
			return errors.New(v.Error())
		case v := <-rmq.cchan.notifyCancel:
			log.Printf("RabbitMQ handleChannel: got notifyCancel event %s", v)
		case v := <-rmq.cchan.notifyFlow:
			rmq.notifyFlowEvent(v)
		case v := <-rmq.cchan.notifyReturn:
			rmq.notifyReturnEvent(v)
		case <-rmq.cchan.stopchannel:
			log.Printf("RabbitMQ handleChannel: Got stop channel event, stopping")
			return
		}
	}

	return nil
} // handleChannel

// Finish - stop work
func (rmq *RabbitMQ) Finish() {
	log.Printf("RabbitMQ Finish: stop work")

	rmq.terminated = true

	for cname, _ := range rmq.consumer {
		log.Printf("RabbitMQ Finish: cancelling consumer %s", cname)
		if err := rmq.Cancel(cname); err != nil {
			log.Printf("RabbitMQ Finish: got error from cancel %s: %v", cname, err)
		}
	}

	if rmq.connected {
		rmq.cchan.stopchannel <- true
	}
	log.Printf("RabbitMQ Finish: closing csend channel")
	close(rmq.Csend)

	log.Printf("RabbitMQ Finish: done")
}

// notifyReturnEvent - not implemented yet
func (rmq *RabbitMQ) notifyReturnEvent(v amqp.Return) {
	log.Printf("RabbitMQ notifyReturnEvent: %+v", v)
} // notifyReurnEvent

// notifyPublishEbemt - not implemented yet
func (rmq *RabbitMQ) notifyPublishEvent(v amqp.Confirmation) {
	log.Printf("RabbitMQ notifyPublishEvent: %+v", v)
} // notifyPublishEvent

// notifyFlowEvent - not implemented yet
func (rmq *RabbitMQ) notifyFlowEvent(v bool) {
	log.Printf("RabbitMQ notifyFlowEvent: %t", v)
} // notifyFlowEvent

// Init - initialize RabbitMQ connection
// create all needed exchanges, queues, bindings and so on
func (rmq *RabbitMQ) Init() (err error) {
	if err = rmq.Connect(); err != nil {
		log.Printf("RabbitMQ Init: error while connecting to RabbitMQ server: %v", err)
		return err
	}

	if err = rmq.declareExchange(rmq.conf.Exchange); err != nil {
		rmq.handleFatalError()
		return err
	}

	// Очереди
	for _, q := range rmq.conf.Exchange.Queue {
		if err = rmq.declareQueue(q.Name, q.Args); err != nil {
			rmq.handleFatalError()
			return err
		}

		for _, b := range q.Binding {
			if err = rmq.bindQueue(rmq.conf.Exchange.Name, b, q.Name); err != nil {
				rmq.handleFatalError()
				return err
			}
		}
	}

	// Отконнектимся
	//	rmq.channel.Close()
	rmq.Finish()

	return nil
}

// decaleQueue - declare new RabbitMQ queue
func (rmq *RabbitMQ) declareQueue(qname string, args map[string]interface{}) (err error) {
	if !rmq.connected {
		return errNotConnected
	}

	var t = make(amqp.Table)
	if args != nil {
		for k, v := range args {
			switch args[k].(type) {
			case int:
				t[k] = int32(v.(int))
			default:
				t[k] = v.(string)
			}
		}
	}

	if _, err := rmq.channel.QueueDeclare(qname, true, false, false, false, t); err != nil {
		log.Printf("RabbitMQ declareQueue: %s error %s", qname, err)
		return err
	}

	log.Printf("RabbitMQ declareQueue: successfully declared %s with args %+v", qname, args)
	return nil
}

// bindQueue - create queue binding
func (rmq *RabbitMQ) bindQueue(ename, rkey, qname string) (err error) {
	if !rmq.connected {
		return errNotConnected
	}

	if err = rmq.channel.QueueBind(qname, rkey, ename, false, nil); err != nil {
		log.Printf("RabbitMQ bindQueue: %s to %s with %s error: %s", qname, ename, rkey, err)
		return err
	}

	log.Printf("RabbitMQ bindQueue: successfully bound %s to %s with %s", qname, ename, rkey)
	return nil
}

// handleFatalError
func (rmq *RabbitMQ) handleFatalError() {
	err := rmq.conn.Close()
	rmq.connected = false
	log.Printf("RabbitMQ handleFatalError: %s", err)
}

// declareEWxchange - create exchange
func (rmq *RabbitMQ) declareExchange(e Exchange) (err error) {
	if !rmq.connected {
		return errNotConnected
	}

	if err = rmq.channel.ExchangeDeclare(e.Name, e.Kind, true, false, false, false, nil); err != nil {
		log.Printf("RabbitMQ declareExchange: %s error %s", e.Name, err)
		return err
	}

	log.Printf("RabbitMQ declareExchange: succesfully declared %s", e.Name)
	return nil
}

// disconnect
func (rmq *RabbitMQ) disconnect() (err error) {
	if !rmq.connected {
		return errNotConnected
	}

	err = rmq.conn.Close()

	return err

} // Disconnect

// Publish - publish message to RabbitMQ with specified parameters
func (rmq *RabbitMQ) Publish(route string, msg *RabbitMQMessage) (e error) {
	if !rmq.connected {
		return errNotConnected
	}

	log.Printf("RabbitMQ Publish: Route %s Exchange %s message Body %s Headers %v Priority %d Expiration %s", route, rmq.conf.Exchange.Name, string(msg.Body), msg.Headers, msg.Priority, msg.Expiration)

	// Ищем дефолтное значение Expiration, если оно нулевое
	if msg.Expiration == "" {
		msg.Expiration = rmq.conf.Exchange.Expiration
	}

	// Ищем дефолтное значение Expiration, если оно нулевое
	if msg.Priority == 0 {
		msg.Priority = rmq.conf.Exchange.Priority
	}

	var headers = make(amqp.Table)
	for k, v := range msg.Headers {
		switch v.(type) {
		case map[string]interface{}:
			var h = make(amqp.Table)
			for k1, v1 := range v.(map[string]interface{}) {
				h[k1] = v1
			}
			headers[k] = h
		default:
			headers[k] = v
		}
	}

	e = rmq.channel.Publish(
		rmq.conf.Exchange.Name,
		route,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: 2,
			Priority:     msg.Priority,
			Expiration:   msg.Expiration,
			ContentType:  "text/plain",
			Body:         msg.Body,
			Headers:      headers,
		},
	)

	if e != nil {
		log.Printf("RabbitMQ Publish: message %v got error %v", msg, e)
	}
	return e
}

// consumeToQueue - consume to given queue by its name, internal
func (rmq *RabbitMQ) consumeToQueue(qname, cname string) (cchan <-chan amqp.Delivery, err error) {
	if !rmq.connected {
		return nil, err
	}

	return rmq.channel.Consume(
		qname,
		cname,
		false, // autoAck
		false, // exclusive
		false, // noLocal
		true,  // noWait
		nil,
	)
} // consumeToQueue

// Consume - consume to given queue by its name
// Данный метод является блокирующим в случае, если подписка успешная
func (rmq *RabbitMQ) Consume(qname string, cname string, fn func(*RabbitMQMessage)) (err error) {

	log.Printf("RabbitMQ Consume: %s %s started", qname, cname)
	rmq.consumer[cname] = &consumer{queue: qname, consume: true}
	var cchan = make(<-chan amqp.Delivery)

	for {
		switch rmq.connected {
		// Не приконнекчены - поспим там внизу
		case false:
		case true:
			// Если при исправном коннекте к кролику consume не прошел - выходим
			if cchan, err = rmq.consumeToQueue(qname, cname); err != nil {
				log.Printf("RabbitMQ Consume: %s %s error %v", qname, cname, err)
				break
			}
			// Подписались, создаем управляющий канал
			// Эта функция блокирующая и выйдет только если ей пошлют сигнал или закроется канал доставок
			// reconsume == true - вышли, так как кончился канал доставок (пытаемся переподписаться)
			// reconsume == fals3 - вышли по сигналу (больше не пытаемся подписаться)
			rmq.handleDelivery(cchan, qname, cname, fn)
		}

		if rmq.terminated {
			break
		}

		if !rmq.consumer[cname].consume {
			break
		}

		tm := time.NewTimer(time.Duration(rmq.conf.ReconsumeDelay) * time.Second)
		<-tm.C
	}

	log.Printf("RabbitMQ Consume: %s %s done", qname, cname)
	return nil
}

// handleDelivery - read events from RabbitMQ, run specified function on every event
func (rmq *RabbitMQ) handleDelivery(cchan <-chan amqp.Delivery, cname, qname string, fn func(*RabbitMQMessage)) {

	log.Printf("RabbitMQ handleDelivery: %s %s started", qname, cname)

	for msg := range cchan {
		//		log.Printf("RabbitMQ handleDelivery: %s %s got message Body %s, Headers %v, Priority %d, Expiration %s DeliveryTag %v RoutingKey %v", qname, cname, string(msg.Body), msg.Headers, msg.Priority, msg.Expiration, msg.DeliveryTag, msg.RoutingKey)
		var answer = new(RabbitMQMessage)
		answer.Headers = make(map[string]interface{})
		for k, v := range msg.Headers {
			switch v.(type) {
			case amqp.Table:
				var h = make(map[string]interface{})
				for k1, v1 := range v.(amqp.Table) {
					h[k1] = v1
				}
				answer.Headers[k] = h
			default:
				answer.Headers[k] = v
			}
		}
		answer.Body = msg.Body
		answer.DeliveryTag = msg.DeliveryTag
		answer.Priority = msg.Priority
		answer.RoutingKey = msg.RoutingKey
		fn(answer)
	}

	log.Printf("RabbitMQ handleDelivery: %s %s done", qname, cname)
}

// Cancel - cancel consumer
func (rmq *RabbitMQ) Cancel(cname string) (err error) {

	if _, ok := rmq.consumer[cname]; !ok {
		return errConsumerNotExists
	}

	log.Printf("RabbitMQ Cancel: consumer %s queue %s", cname, rmq.consumer[cname].queue)

	if !rmq.consumer[cname].consume {
		return errNotConsumed
	}

	// Выставляем флаг не подписываться больше
	rmq.consumer[cname].consume = false

	// Вызываем channel.Cancel
	if rmq.connected {
		if err = rmq.channel.Cancel(cname, false); err != nil {
			log.Printf("RabbitMQ Cancel: consumer %s error %s", cname, err)
		}
	}

	return nil
}

// Ack - send Ack
func (rmq *RabbitMQ) Ack(tag interface{}) {
	if !rmq.connected {
		return
	}

	if tag == nil {
		return
	}
	rmq.channel.Ack(tag.(uint64), false)
} // Ack

// Nack - send Nack
func (rmq *RabbitMQ) Nack(tag interface{}) {
	if !rmq.connected {
		return
	}

	log.Printf("RabbitMQ Nack: delivery tag %d", tag)
	if tag == nil {
		log.Printf("RabbitMQ Nack: Empty DeliveryTag!")
		return
	}
	rmq.channel.Nack(tag.(uint64), false, true)
} // Nack

// Flow - start/stop flow
func (rmq *RabbitMQ) Flow(active bool) (err error) {
	if !rmq.connected {
		return errNotConnected
	}

	log.Printf("RabbitMQ Flow: start active %v paused %v", active, rmq.paused)
	// Статус уже и так выставлен
	if active == !rmq.paused {
		return nil
	}

	if err = rmq.channel.Flow(active); err != nil {
		log.Printf("RabbitMQ Flow: got error %v from channel Flow for active %v", err, active)
		return err
	}

	rmq.paused = !active
	log.Printf("RabbitMQ Flow: done active %v paused %v", active, rmq.paused)

	return nil
} // Flow

// ConnectToRabbitMQ - connect to RabbitMQ broker with given settings
func ConnectToRabbitMQ(conf RabbitMQConfig) (rmq *RabbitMQ, err error) {

	// Коннектимся к кролику
	rmq = New(conf)
	if err = rmq.Connect(); err != nil {
		log.Printf("RabbitMQ ConnectToRabbitMQ: unable to connect to RabbitMQ server: %s", err)
		return nil, err
	}

	return rmq, nil

} // ConnectToRabbitMQ

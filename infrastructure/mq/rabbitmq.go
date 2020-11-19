package mq

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.ErrorLevel)

	viper.SetConfigName("local")
	viper.AddConfigPath(".")
	viper.SetConfigType("toml")
	viper.AllowEmptyEnv(true)
	_ = viper.ReadInConfig()
}

var (
	// MQURL 地址
	MQURL = viper.GetString("amqp.rabbitmq")
)

// MQHandler 控制器
type MQHandler struct {
	Object *RabbitMQ
}

// RabbitMQ 对象
type RabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel

	QueueName string
	Exchange  string
	Key       string
	URL       string
}

// NewRabbitMQ 创建基本对象
func NewRabbitMQ(queueName, exchange, key string) *RabbitMQ {
	mq := &RabbitMQ{
		QueueName: queueName,
		Exchange:  exchange,
		Key:       key,
		URL:       MQURL,
	}

	var err error
	mq.conn, err = amqp.Dial(mq.URL)
	mq.failOnErr(err, "Create connection failed")
	mq.channel, err = mq.conn.Channel()
	mq.failOnErr(err, "Get channel failed")

	return mq
}

// Destroy 销毁
func (r *RabbitMQ) Destroy() {
	r.channel.Close()
	r.conn.Close()
}

func (r *RabbitMQ) failOnErr(err error, reason string) {
	if err != nil {
		log.WithFields(log.Fields{
			"err":    err,
			"reason": reason,
		}).Fatal("RabbitMQ failed on:")
	}
}

// -----------------------------------------------------------

// NewRabbitMQSimple 简单对象
func NewRabbitMQSimple(queueName string) *RabbitMQ {
	return NewRabbitMQ(queueName, "", "")
}

// PublishSimple 简单发布
func (r *RabbitMQ) PublishSimple(message string) {
	_, err := r.channel.QueueDeclare(r.QueueName, false, false, false, false, nil)

	if err != nil {
		r.failOnErr(err, "Failed declare queue in PublishSimple")
	}

	r.channel.Publish(r.Exchange, r.QueueName, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(message),
	})
}

// ConsumeSimple 简单消费
func (r *RabbitMQ) ConsumeSimple() {
	_, err := r.channel.QueueDeclare(r.QueueName, false, false, false, false, nil)
	if err != nil {
		r.failOnErr(err, "Failed declare queue in ConsumeSimple")
	}

	messages, err := r.channel.Consume(r.QueueName, "", true, false, false, false, nil)
	if err != nil {
		r.failOnErr(err, "Failed consume in ConsumeSimple")
	}

	forever := make(chan bool)
	go func() {
		for d := range messages {
			// logic
			fmt.Printf("Received a message %s\n", d.Body)
		}
	}()

	println("[*] Waiting for messages, to exit press CTRL+C")
	<-forever
}

// ---------------------------------------------------------

// NewRabbitMQPubSub 发布订阅对象
func NewRabbitMQPubSub(exchangeName string) *RabbitMQ {
	return NewRabbitMQ("", exchangeName, "")
}

// PublishPub 发布
func (r *RabbitMQ) PublishPub(message string) {
	err := r.channel.ExchangeDeclare(r.Exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		r.failOnErr(err, "Failed declare exchange in PublishPub")
	}

	err = r.channel.Publish(r.Exchange, "", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(message),
	})
	if err != nil {
		r.failOnErr(err, "Failed publish in PublishPub")
	}
}

// ReceiveSub 订阅接收
func (r *RabbitMQ) ReceiveSub() {
	err := r.channel.ExchangeDeclare(r.Exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		r.failOnErr(err, "Failed declare exchange in ReceiveSub")
	}

	q, err := r.channel.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		r.failOnErr(err, "Failed declare queue in ReceiveSub")
	}

	err = r.channel.QueueBind(q.Name, "", r.Exchange, false, nil)
	if err != nil {
		r.failOnErr(err, "Failed bind queue in ReceiveSub")
	}

	messages, err := r.channel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		r.failOnErr(err, "Failed consume in ReceiveSub")
	}

	forever := make(chan bool)
	go func() {
		for d := range messages {
			fmt.Printf("Received a message: %s\n", d.Body)
		}
	}()

	println("[*] Waiting for messages, to exit press CTRL+C")
	<-forever
}

// ----------------------------------------------------------

// NewRabbitMQRouting 路由分发
func NewRabbitMQRouting(exchangeName, routingKey string) *RabbitMQ {
	return NewRabbitMQ("", exchangeName, routingKey)
}

// PublishRouting 路由分发
func (r *RabbitMQ) PublishRouting(message string) {
	err := r.channel.ExchangeDeclare(r.Exchange, "direct", true, false, false, false, nil)
	if err != nil {
		r.failOnErr(err, "Failed declare exchange in PublishRouting")
	}

	err = r.channel.Publish(r.Exchange, r.Key, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(message),
	})
	if err != nil {
		r.failOnErr(err, "Failed publish in PublishRouting")
	}
}

// ReceiveRouting 路由接收
func (r *RabbitMQ) ReceiveRouting() {
	err := r.channel.ExchangeDeclare(r.Exchange, "direct", true, false, false, false, nil)
	if err != nil {
		r.failOnErr(err, "Failed declare exchange in ReceiveRouting")
	}

	q, err := r.channel.QueueDeclare("", false, false, false, false, nil)
	if err != nil {
		r.failOnErr(err, "Failed delcare queue in ReceiveRouting")
	}

	err = r.channel.QueueBind(q.Name, r.Key, r.Exchange, false, nil)
	if err != nil {
		r.failOnErr(err, "Failed bind queue in ReceiveRouting")
	}

	messages, err := r.channel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		r.failOnErr(err, "Failed consume in ReceiveRouting")
	}

	forever := make(chan bool)
	go func() {
		for d := range messages {
			fmt.Printf("Received a message: %s\n", d.Body)
		}
	}()

	println("[*] Waiting for messages, to exit press CTRL+C")
	<-forever
}

package mq

import (
	"github.com/streadway/amqp"
	"github.com/zvnlanx/mq/invariant"
)

// MQHandler 队列Handler
type MQHandler struct {
	conn    *amqp.Connection
	channel *amqp.Channel

	QueueName string
	Exchange  string
	Key       string
	URL       string
	done      chan error
}

// NewMQHandler 创建基本对象
func NewMQHandler(queueName, exchange, key string) (*MQHandler, error) {
	mq := &MQHandler{
		QueueName: queueName,
		Exchange:  exchange,
		Key:       key,
		URL:       invariant.RabbitMQURL,
		done:      make(chan error),
	}

	var err error
	mq.conn, err = amqp.Dial(mq.URL)
	if err != nil {
		return nil, err
	}
	mq.channel, err = mq.conn.Channel()
	if err != nil {
		return nil, err
	}
	return mq, nil
}

// Destroy 销毁
func (r *MQHandler) Destroy() {
	<-r.done
	r.channel.Close()
	r.conn.Close()
}

// -----------------------------------------------------------

// NewMQHandlerSimple 简单队列
func NewMQHandlerSimple(queueName string) (*MQHandler, error) {
	return NewMQHandler(queueName, "", "")
}

// Push 简单发布
func (r *MQHandler) Push(message string) error {
	// 声明发送到的队列
	_, err := r.channel.QueueDeclare(
		r.QueueName, // queue name
		false,       // durable  持久化
		false,       // delete when unused
		false,       // exclusive 单个连接使用queue，在连接断开时自动删除
		false,       // no-wait
		nil,         // arguments 队列类型、消息TTL、队列长度限制等
	)
	if err != nil {
		return err
	}

	// 发送消息
	r.channel.Publish(
		r.Exchange,  // exchange
		r.QueueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)

	return nil
}

// Consume 简单消费
func (r *MQHandler) Consume(handle func(<-chan amqp.Delivery, chan error)) error {
	// 声明消费的队列，确定消费前队列存在
	_, err := r.channel.QueueDeclare(r.QueueName, false, false, false, false, nil)
	if err != nil {
		return err
	}
	// 通知服务器通过队列异步分发消息
	deliveries, err := r.channel.Consume(
		r.QueueName, // queue
		"",          // consumer
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return err
	}

	go handle(deliveries, r.done)

	return nil
}

// ---------------------------------------------------------

// NewMQHandlerWorkQueue 多工作队列
func NewMQHandlerWorkQueue(queueName string) (*MQHandler, error) {
	return NewMQHandler(queueName, "", "")
}

// WorkerPublish 发布
func (r *MQHandler) WorkerPublish(message string) error {
	// 声明发送到的队列
	_, err := r.channel.QueueDeclare(
		r.QueueName, // name
		true,        // durable 1.队列持久化
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		return err
	}

	// 发送消息
	r.channel.Publish(
		r.Exchange,  // exchange，若为''则使用默认exchange
		r.QueueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // 2.消息持久化
			ContentType:  "text/plain",
			Body:         []byte(message),
		},
	)

	return nil
}

// WorkerConsume 消费worker
func (r *MQHandler) WorkerConsume(handle func(<-chan amqp.Delivery, chan error)) error {
	// 声明消费的队列，确定消费前队列存在
	_, err := r.channel.QueueDeclare(r.QueueName, false, false, false, false, nil)
	if err != nil {
		return err
	}

	// 配置预取
	if err = r.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	); err != nil {
		return err
	}

	// 通知服务器通过队列异步分发消息
	deliveries, err := r.channel.Consume(
		r.QueueName, // queue
		"",          // consumer
		true,        // auto-ack 若手工ack，这里设置false
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return err
	}

	go handle(deliveries, r.done)

	return nil
}

// ---------------------------------------------------------

// NewMQHandlerPubSub 发布订阅
func NewMQHandlerPubSub(exchangeName string) (*MQHandler, error) {
	return NewMQHandler("", exchangeName, "")
}

// Publish 发布
func (r *MQHandler) Publish(message string) error {
	err := r.channel.ExchangeDeclare(
		r.Exchange, // exchange name
		"fanout",   // type
		true,       // durable
		false,      // auto-deleted 无订阅时自动删除
		false,      // internal
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}

	err = r.channel.Publish(
		r.Exchange, // exchange name
		"",         // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)
	if err != nil {
		return err
	}
	return nil
}

// Subscribe 订阅
func (r *MQHandler) Subscribe(handle func(<-chan amqp.Delivery, chan error)) error {
	err := r.channel.ExchangeDeclare(
		r.Exchange, // exchange name
		"fanout",   // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}

	q, err := r.channel.QueueDeclare(
		"",    // temporary queue name
		false, // durable
		false, // delete when unused
		true,  // exclusive 仅能被声明该Queue的conn操作
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return err
	}

	err = r.channel.QueueBind(
		q.Name,     // queue name
		"",         // routing key
		r.Exchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	deliveries, err := r.channel.Consume(
		q.Name, // queue name
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return err
	}

	go handle(deliveries, r.done)

	return nil
}

// ----------------------------------------------------------

// NewMQHandlerRouting 路由分发
func NewMQHandlerRouting(exchangeName string) (*MQHandler, error) {
	return NewMQHandler("", exchangeName, "")
}

// RoutingPublish 路由分发
func (r *MQHandler) RoutingPublish(message, routingKey string) error {
	err := r.channel.ExchangeDeclare(
		r.Exchange, // exchange name
		"direct",   // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}

	err = r.channel.Publish(
		r.Exchange, // exchange name
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)
	if err != nil {
		return err
	}
	return nil
}

// RoutingConsume 消费路由
func (r *MQHandler) RoutingConsume(bindingKeys []string, handle func(<-chan amqp.Delivery, chan error)) error {
	err := r.channel.ExchangeDeclare(r.Exchange, "direct", true, false, false, false, nil)
	if err != nil {
		return err
	}

	q, err := r.channel.QueueDeclare("", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for _, bindingKey := range bindingKeys {
		err = r.channel.QueueBind(
			q.Name,     // temporary queue name
			bindingKey, // binding key
			r.Exchange, // exchange name
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	deliveries, err := r.channel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	go handle(deliveries, r.done)

	return nil
}

// ----------------------------------------------------------

// NewMQHandlerTopic 基于Topic分发
func NewMQHandlerTopic(exchangeName string) (*MQHandler, error) {
	return NewMQHandler("", exchangeName, "")
}

// TopicPublish Topic发布
func (r *MQHandler) TopicPublish(message, routingTopic string) error {
	if err := r.channel.ExchangeDeclare(
		r.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	if err := r.channel.Publish(
		r.Exchange,
		routingTopic,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	); err != nil {
		return err
	}

	return nil
}

// TopicConsume Topic消费
func (r *MQHandler) TopicConsume(bindingTopics []string, handle func(<-chan amqp.Delivery, chan error)) error {
	err := r.channel.ExchangeDeclare(r.Exchange, "topic", true, false, false, false, nil)
	if err != nil {
		return err
	}

	q, err := r.channel.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		return err
	}

	for _, bindingTopic := range bindingTopics {
		err = r.channel.QueueBind(q.Name, bindingTopic, r.Exchange, false, nil)
		if err != nil {
			return err
		}
	}

	deliveries, err := r.channel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	go handle(deliveries, r.done)

	return nil
}

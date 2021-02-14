package google_pubsub

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/pfortin-urbn/stalk/collectors"
)

type PubSubCollector struct {
	*collectors.BaseCollector
	PubSubClient *pubsub.Client
	PollingLimit int

	GCPSourceTopic *pubsub.Topic
	GCPErrorTopic  *pubsub.Topic
	Subscript      *pubsub.Subscription
}

func CreatePubSubCollector(collectorOptions collectors.CollectorOptions) (*PubSubCollector, error) {
	// gcloud credentials are in json file locally
	client, err := pubsub.NewClient(context.Background(), collectorOptions.AccountID)
	if err != nil {
		return nil, err
	}

	c := &PubSubCollector{
		PubSubClient: client,
		PollingLimit: collectorOptions.PollingLimit,
	}

	c.GCPSourceTopic = client.Topic(collectorOptions.SourceTopic)
	c.GCPErrorTopic = client.Topic(collectorOptions.ErrorTopic)

	c.Subscript, err = client.CreateSubscription(context.Background(), collectorOptions.SourceSubscription, pubsub.SubscriptionConfig{
		Topic:                 c.GCPSourceTopic,
		AckDeadline:           time.Duration(collectorOptions.RetryIntervalSecs) * time.Second,
		RetainAckedMessages:   false,
		EnableMessageOrdering: false,
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     c.GCPErrorTopic.String(),
			MaxDeliveryAttempts: 5,
		},
		RetryPolicy: &pubsub.RetryPolicy{
			MinimumBackoff: time.Duration(collectorOptions.RetryIntervalSecs),
			MaximumBackoff: time.Duration(collectorOptions.RetryIntervalSecs),
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "Resource already exists") {
			c.Subscript = client.Subscription(collectorOptions.SourceSubscription)
		} else {
			panic(err)
		}
	}
	c.Subscript.ReceiveSettings.MaxOutstandingMessages = 1

	collectorOptions.AckMessage = c.AckMessage
	collectorOptions.GetMessages = c.GetMessages
	collectorOptions.PublishMessage = c.PublishMessage
	collectorOptions.Sleep = c.Sleep
	collectorOptions.Wake = c.Wake

	c.BaseCollector = collectors.CreateBaseCollector(collectorOptions)

	go c.PollForMessages()
	return c, nil
}

func (collector *PubSubCollector) Sleep() {
	log.Println("Sleeping")
	collector.Sleeping = true
}

func (collector *PubSubCollector) Wake() {
	log.Println("Waking up")
	collector.Sleeping = false
	go collector.timeout()
}

func (collector *PubSubCollector) timeout() {
	time.Sleep(time.Duration(collector.PollingPeriod) * time.Millisecond)
	collector.ChannelToInitiatePollRequest <- true
}

func (collector *PubSubCollector) PollForMessages() {
	go collector.timeout()
	for {
		select {
		case <-collector.ChannelToInitiatePollRequest:
			if !collector.BaseCollector.Sleeping {
				_, err := collector.GetMessages()
				collector.ProcessExponentialBackoff(err)
			}
			go collector.timeout()
		case <-collector.ChannelDone:
			return
		}
	}
}

func (collector *PubSubCollector) Handler(ctx context.Context, msg *pubsub.Message) {
	// If value is not present in map we send an empty string in here, which will result in a 0 int.
	retries, _ := strconv.Atoi(msg.Attributes["retries"])
	rightNow := time.Now()
	diff := int64(rightNow.Sub(msg.PublishTime).Seconds())
	if retries != 0 && diff < collector.RetryIntervalSecs {
		log.Println("nacking msg id:", msg.ID)
		msg.Nack()
		return
	}

	log.Printf("Got message: %q(%d)\n", string(msg.Data), retries)

	wrappedMessage := collectors.MessageWrapper{
		MessageBody: msg.Data,
		Retries:     retries,
		Retry:       true,
		Fatal:       false,
		Message:     msg,
	}
	//msg.Ack()
	result := collector.BusinessProcessor(msg.Data)
	collector.ProcessMessageResult(wrappedMessage, result)
}

func (collector *PubSubCollector) GetMessages() ([]collectors.MessageWrapper, error) {
	var waitTimeSecs int64 = 10 //TODO make a collectorOption?
	log.Printf("Get messages start")
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(waitTimeSecs)*time.Second)

	err := collector.Subscript.Receive(ctx, collector.Handler)
	collector.ProcessExponentialBackoff(err)

	log.Printf("Get messages complete")
	return nil, err
}

func (collector *PubSubCollector) PublishMessage(message *collectors.MessageWrapper, delaySeconds int64, errFlag bool) error {
	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(600*time.Second))
	topic := collector.GCPSourceTopic
	topicName := collector.SourceTopic
	if errFlag {
		topic = collector.GCPErrorTopic
		topicName = collector.ErrorTopic
	}
	////defer topic.Stop()
	message_attrs := make(map[string]string, 0)
	message_retries := 0
	if message.Retries >= 0 {
		message_retries = message.Retries
	}
	message_attrs["retries"] = strconv.FormatInt(int64(message_retries), 10)

	new_message := &pubsub.Message{
		Data:       message.MessageBody,
		Attributes: message_attrs,
	}

	//if delaySeconds > 0 {
	//	log.Printf("Message being delayed here.")
	//	time.Sleep(time.Duration(delaySeconds) * time.Second)
	//}

	result := topic.Publish(context.Background(), new_message)
	// This will block until a result is returned from Google
	_, err := result.Get(ctx)
	if err != nil {
		// TODO - need some way to retry publishes
		log.Printf("Error %s publishing message: %+v to %s\n", err.Error(), message.MessageBody, topicName)
		return err
	}
	log.Printf("Wait Published message to %s\n", topicName)
	return nil
}

func (collector *PubSubCollector) AckMessage(message collectors.MessageWrapper) error {
	// Type assert here to make sure go knows what type we're talking about
	//msg := message.Message.(*pubsub.Message)
	message.Message.(*pubsub.Message).Ack()
	return nil
}

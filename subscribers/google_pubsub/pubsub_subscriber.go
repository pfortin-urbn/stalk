package google_pubsub

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/pfortin-urbn/stalk/subscribers"
)

type PubSubSubScriber struct {
	*subscribers.BaseSubscriber
	PubSubClient *pubsub.Client
	PollingLimit int

	GCPSourceTopic *pubsub.Topic
	GCPErrorTopic  *pubsub.Topic
	Subscript      *pubsub.Subscription
}

func CreatePubSubSubscriber(subscriberOptions subscribers.SubscriberOptions) (*PubSubSubScriber, error) {
	// gcloud credentials are in json file locally
	client, err := pubsub.NewClient(context.Background(), subscriberOptions.AccountID)
	if err != nil {
		return nil, err
	}

	c := &PubSubSubScriber{
		PubSubClient: client,
		PollingLimit: subscriberOptions.PollingLimit,
	}

	c.GCPSourceTopic = client.Topic(subscriberOptions.SourceTopic)
	c.GCPErrorTopic = client.Topic(subscriberOptions.ErrorTopic)

	c.Subscript, err = client.CreateSubscription(context.Background(), subscriberOptions.SourceSubscription, pubsub.SubscriptionConfig{
		Topic:                 c.GCPSourceTopic,
		AckDeadline:           time.Duration(subscriberOptions.RetryIntervalSecs) * time.Second,
		RetainAckedMessages:   false,
		EnableMessageOrdering: false,
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     c.GCPErrorTopic.String(),
			MaxDeliveryAttempts: 5,
		},
		RetryPolicy: &pubsub.RetryPolicy{
			MinimumBackoff: time.Duration(subscriberOptions.RetryIntervalSecs),
			MaximumBackoff: time.Duration(subscriberOptions.RetryIntervalSecs),
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "Resource already exists") {
			c.Subscript = client.Subscription(subscriberOptions.SourceSubscription)
		} else {
			panic(err)
		}
	}
	c.Subscript.ReceiveSettings.MaxOutstandingMessages = 1

	subscriberOptions.AckMessage = c.AckMessage
	subscriberOptions.GetMessages = c.GetMessages
	subscriberOptions.PublishMessage = c.PublishMessage
	subscriberOptions.Sleep = c.Sleep
	subscriberOptions.Wake = c.Wake

	c.BaseSubscriber = subscribers.CreateBaseCollector(subscriberOptions)

	go c.PollForMessages()
	return c, nil
}

func (collector *PubSubSubScriber) Sleep() {
	log.Println("Sleeping")
	collector.Sleeping = true
}

func (collector *PubSubSubScriber) Wake() {
	log.Println("Waking up")
	collector.Sleeping = false
	go collector.timeout()
}

func (collector *PubSubSubScriber) timeout() {
	time.Sleep(time.Duration(collector.PollingPeriod) * time.Millisecond)
	collector.ChannelToInitiatePollRequest <- true
}

func (collector *PubSubSubScriber) PollForMessages() {
	go collector.timeout()
	for {
		select {
		case <-collector.ChannelToInitiatePollRequest:
			if !collector.BaseSubscriber.Sleeping {
				_, err := collector.GetMessages()
				collector.ProcessExponentialBackoff(err)
			}
			go collector.timeout()
		case <-collector.ChannelDone:
			return
		}
	}
}

func (collector *PubSubSubScriber) Handler(ctx context.Context, msg *pubsub.Message) {
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

	wrappedMessage := subscribers.MessageWrapper{
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

func (collector *PubSubSubScriber) GetMessages() ([]subscribers.MessageWrapper, error) {
	var waitTimeSecs int64 = 10 //TODO make a collectorOption?
	log.Printf("Get messages start")
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(waitTimeSecs)*time.Second)

	err := collector.Subscript.Receive(ctx, collector.Handler)
	collector.ProcessExponentialBackoff(err)

	log.Printf("Get messages complete")
	return nil, err
}

func (collector *PubSubSubScriber) PublishMessage(message *subscribers.MessageWrapper, delaySeconds int64, errFlag bool) error {
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

func (collector *PubSubSubScriber) AckMessage(message subscribers.MessageWrapper) error {
	// Type assert here to make sure go knows what type we're talking about
	//msg := message.Message.(*pubsub.Message)
	message.Message.(*pubsub.Message).Ack()
	return nil
}

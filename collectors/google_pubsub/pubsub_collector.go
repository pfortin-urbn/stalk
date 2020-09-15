package google_pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/pfortin-urbn/stalk/collectors"
	"log"
	"strconv"
	"time"
)

type PubSubCollector struct {
	*collectors.BaseCollector
	PubSubClient       *pubsub.Client
	PollingLimit       int
	SourceSubscription string
}

func CreatePubSubCollector(collectorOptions collectors.CollectorOptions) (*PubSubCollector, error) {
	// gcloud credentials are in json file locally
	client, err := pubsub.NewClient(context.Background(), collectorOptions.AccountID)
	if err != nil {
		return nil, err
	}

	c := &PubSubCollector{
		BaseCollector:      collectors.CreateBaseCollector(collectorOptions),
		PubSubClient:       client,
		PollingLimit:       collectorOptions.PollingLimit,
		SourceSubscription: collectorOptions.SourceSubscription,
	}

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

func (collector *PubSubCollector) HandleMessage(message *pubsub.Message) error {
	result := collector.BusinessProcessor(message.Data)
	if result.Err != nil {
		msg := collectors.MessageWrapper{
			MessageBody: message.Data,
			Retries:     *message.DeliveryAttempt,
			Retry:       result.Retry,
			Fatal:       result.Fatal,
		}
		collector.ProcessMessageResult(msg, result)
	}
	return result.Err
}

func (collector *PubSubCollector) PollForMessages() {
	go collector.timeout()
	for {
		select {
		case <-collector.ChannelToInitiatePollRequest:
			if !collector.BaseCollector.Sleeping {
				messages, err := collector.GetMessages()
				collector.ProcessExponentialBackoff(err)
				for _, msg := range messages {
					result := collector.BusinessProcessor(msg.MessageBody)
					collector.ProcessMessageResult(msg, result)
				}
			}
			go collector.timeout()
		case <-collector.ChannelDone:
			return
		}
	}
}

func (collector *PubSubCollector) timeout() {
	time.Sleep(time.Duration(collector.PollingPeriod) * time.Millisecond)
	collector.ChannelToInitiatePollRequest <- true
}

func (collector *PubSubCollector) GetMessages() ([]collectors.MessageWrapper, error) {
	log.Printf("Get messages start")
	ctx, cancel := context.WithCancel(context.Background())

	// There's a built in deadline on the subscription that will orphan the message in the subscription.
	subscription := collector.PubSubClient.Subscription(collector.SourceSubscription)
	messages := make([]collectors.MessageWrapper, 0)

	// Track number of messages received and timer to cancel this function when needed
	found := make(chan int, 1)
	received := 0
	timer := time.NewTimer(time.Duration(collector.PollingPeriod) * time.Second)
	go func() {
		for {
			select {
			case <-found:
				received++
				if received >= collector.PollingLimit {
					log.Printf("Recieved %s messages\n", collector.PollingLimit)
					cancel()
				}
			case <-timer.C:
				log.Println("Timer fired")
				cancel()
			}
		}
	}()

	err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		log.Printf("Got message: %q\n", string(msg.Data))
		// If value is not present in map we send an empty string in here, which will result in a 0 int.
		retries, _ := strconv.Atoi(msg.Attributes["retries"])

		wrappedMessage := collectors.MessageWrapper{
			MessageBody: msg.Data,
			Retries:     retries,
			Retry:       true,
			Fatal:       false,
			Message:     msg,
		}
		messages = append(messages, wrappedMessage)
		//collector.AckMessage(wrappedMessage)
		msg.Nack()
		found <- 1
	})
	if err != nil {
		log.Printf("Could not recieve messages from - %s : %v", collector.SourceSubscription, err)
	}
	log.Printf("Get messages complete")
	return messages, err
}

func (collector *PubSubCollector) PublishMessage(message *collectors.MessageWrapper, delaySeconds int64, errFlag bool) error {
	ctx := context.Background()
	topic := collector.PubSubClient.Topic(collector.SourceTopic)
	topicName := collector.SourceTopic
	if errFlag {
		topic = collector.PubSubClient.Topic(collector.ErrorTopic)
		topicName = collector.ErrorTopic
	}
	// Clean up goroutines for batching and sending messages
	defer topic.Stop()

	message_attrs := make(map[string]string, 0)
	message_retries := 0
	if message.Retries >= 0 {
		message_retries = message.Retries
	}
	message_attrs["retries"] = strconv.FormatInt(int64(message_retries), 10)

	if delaySeconds > 0 {
		time.Sleep(time.Duration(delaySeconds) * time.Second)
	}

	result := topic.Publish(ctx, &pubsub.Message{
		Data:       message.MessageBody,
		Attributes: message_attrs,
	})
	// This will block until a result is returned from Google
	id, err := result.Get(ctx)
	if err != nil {
		// TODO - need some way to retry publishes
		log.Printf("Error publishing message: %+v to %s\n", message.MessageBody, topicName)
		return err
	}
	log.Printf("Published message - id: %s to %s\n", id, topicName)
	return nil
}

func (collector *PubSubCollector) AckMessage(message collectors.MessageWrapper) error {
	// Type assert here to make sure go knows what type we're talking about
	msg := message.Message.(*pubsub.Message)
	msg.Ack()
	return nil
}

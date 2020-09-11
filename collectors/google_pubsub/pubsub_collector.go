package google_pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/pfortin-urbn/stalk/collectors"
	"log"
	"sync"
)

type PubSubCollector struct {
	*collectors.BaseCollector
	PubSubClient       *pubsub.Client
	PollingLimit       int
	SourceSubscription string
	Context            context.Context
	Cancel             context.CancelFunc
}

func CreatePubSubCollector(collectorOptions collectors.CollectorOptions) (*PubSubCollector, error) {
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
	// QUESTION - should I add the channel usage back in here like in AWS Collector?
	for {
		if !collector.BaseCollector.Sleeping {
			log.Println("Processing messages...")
			messages, err := collector.GetMessages()
			if err != nil {
				log.Println("Error recieving messages - retrying")
				collector.ProcessExponentialBackoff(err)
			}
			for _, msg := range messages {
				collector.BusinessProcessor(msg.MessageBody)
			}
			log.Printf("Recieved messages - %s\n", messages)
		}
	}
}

func (collector *PubSubCollector) GetMessages() ([]collectors.MessageWrapper, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// There's a built in deadline on the subscription that will orphan the message in the subscription.
	subscription := collector.PubSubClient.Subscription(collector.SourceSubscription)
	messages := make([]collectors.MessageWrapper, 0)

	// NOTE - HTF is this referencing the subscription.Mutex?!?!?!?!
	var mu sync.Mutex
	err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()

		received := 0
		log.Printf("Got message: %q\n", string(msg.Data))
		messages = append(messages, collectors.MessageWrapper{
			MessageBody: msg.Data,
			Retry:       false,
			Fatal:       false,
		})
		msg.Ack()

		received++
		if received >= collector.PollingLimit {
			cancel()
		}
	})
	if err != nil {
		log.Printf("Could not recieve messages from - %s : %v", collector.SourceSubscription, err)
	}
	return messages, err
}

func (collector *PubSubCollector) PublishMessage(message *collectors.MessageWrapper, delaySeconds int64, errFlag bool) error {
	ctx := context.Background()
	topic := collector.PubSubClient.Topic(collector.SourceTopic)
	if errFlag {
		topic = collector.PubSubClient.Topic(collector.ErrorTopic)
	}
	// Clean up goroutines for batching and sending messages
	defer topic.Stop()

	result := topic.Publish(ctx, &pubsub.Message{
		Data: message.MessageBody,
	})
	// This will block until a result is returned from Google
	id, err := result.Get(ctx)
	if err != nil {
		// TODO - need some way to retry publishes
		log.Printf("Error publishing retry message: %+v\n", pubsub.Message)
		return err
	}
	log.Printf("Published message - id: %s\n", id)
	//var err error
	return nil
}

func (collector *PubSubCollector) AckMessage(receiptHandle string) error {
	//	FIXME - there is a way to ACK this but you need the message you received to be able to do it.
	//  	msg.Ack()
	return nil
}

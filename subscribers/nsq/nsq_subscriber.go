package nsq

import (
	"os"
	"time"

	"github.com/nsqio/go-nsq"

	"github.com/pfortin-urbn/stalk/subscribers"
)

var nsqHost = os.Getenv("NSQ_HOST")

type NsqSubscriber struct {
	scale int
	*subscribers.BaseSubscriber
	Consumer *nsq.Consumer
	Producer *nsq.Producer
}

// CreateNsqSubscriber -
func CreateNsqSubscriber(subscriberOptions subscribers.SubscriberOptions) (*NsqSubscriber, error) {
	var err error
	nsqCollector := &NsqSubscriber{}

	config := nsq.NewConfig()
	config.MaxAttempts = uint16(subscriberOptions.MaxRetries)
	config.DefaultRequeueDelay = time.Duration(subscriberOptions.RetryIntervalSecs) * time.Second
	// TODO channel(consumer group) should be configured in subscriberOptions
	nsqCollector.Consumer, err = nsq.NewConsumer(subscriberOptions.SourceTopic, "stalk", config)
	if err != nil {
		return nil, err
	}
	nsqCollector.Producer, err = nsq.NewProducer(nsqHost, config)
	if err != nil {
		return nil, err
	}

	nsqCollector.Consumer.ChangeMaxInFlight(0)
	nsqCollector.Consumer.AddConcurrentHandlers(nsqCollector, 1)

	err = nsqCollector.Consumer.ConnectToNSQD(nsqHost)
	if err != nil {
		return nil, err
	}

	subscriberOptions.AckMessage = nsqCollector.AckMessage
	subscriberOptions.GetMessages = nsqCollector.GetMessages
	subscriberOptions.PublishMessage = nsqCollector.PublishMessage
	subscriberOptions.Wake = nsqCollector.Wake
	subscriberOptions.Sleep = nsqCollector.Sleep

	nsqCollector.BaseSubscriber = subscribers.CreateBaseCollector(subscriberOptions)

	nsqCollector.scale = 1

	return nsqCollector, nil
}

// Sleep -
func (collector *NsqSubscriber) Sleep() {
	collector.Sleeping = true
	collector.Consumer.ChangeMaxInFlight(0)
}

// Wake -
func (collector *NsqSubscriber) Wake() {
	collector.Sleeping = false
	collector.Consumer.ChangeMaxInFlight(collector.scale)
}

func (collector *NsqSubscriber) Scale(numCollectors int) bool {
	collector.scale = numCollectors
	collector.Consumer.ChangeMaxInFlight(collector.scale)
	return true
}

// HandleMessage - Received messages from NSQ one at a time.
func (collector *NsqSubscriber) HandleMessage(message *nsq.Message) error {
	result := collector.BusinessProcessor(message.Body)
	if result.Err != nil {
		msg := subscribers.MessageWrapper{
			Message:     message,
			MessageBody: message.Body,
			Retries:     int(message.Attempts),
			Retry:       result.Retry,
			Fatal:       result.Fatal,
		}
		collector.ProcessMessageResult(msg, result)
	}
	return nil
}

// GetMessages - Not used for the NSQ Subscriber since it has a callback above (HandleMessage)
//
//	and handles it's own exponential backoff as well.
func (collector *NsqSubscriber) GetMessages() ([]subscribers.MessageWrapper, error) {
	messages := make([]subscribers.MessageWrapper, 0)

	return messages, nil
}

// PublishMessage -
func (collector *NsqSubscriber) PublishMessage(message *subscribers.MessageWrapper, _ int64, errFlag bool) error {

	if errFlag {
		return collector.Producer.Publish(collector.ErrorTopic, message.MessageBody)
	}
	message.Message.(*nsq.Message).RequeueWithoutBackoff(time.Duration(collector.RetryIntervalSecs) * time.Second)
	return nil
}

// AckMessage - Not needed for NSQ (Messages are not acknowledged
func (collector *NsqSubscriber) AckMessage(message subscribers.MessageWrapper) error {
	return nil
}

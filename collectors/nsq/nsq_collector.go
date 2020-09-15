package nsq

import (
	"os"
	"time"

	"github.com/nsqio/go-nsq"

	"github.com/pfortin-urbn/stalk/collectors"
)

var nsqHost = os.Getenv("NSQ_HOST")

type NsqCollector struct {
	scale int
	*collectors.BaseCollector
	Consumer *nsq.Consumer
	Producer *nsq.Producer
}

//CreateNsqCollector -
func CreateNsqCollector(collectorOptions collectors.CollectorOptions) (*NsqCollector, error) {
	var err error
	nsqCollector := &NsqCollector{}

	config := nsq.NewConfig()
	config.MaxAttempts = uint16(collectorOptions.MaxRetries)
	config.DefaultRequeueDelay = time.Duration(collectorOptions.RetryIntervalSecs) * time.Second
	// TODO channel(consumer group) should be configured in collectorOptions
	nsqCollector.Consumer, err = nsq.NewConsumer(collectorOptions.SourceTopic, "stalk", config)
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

	collectorOptions.AckMessage = nsqCollector.AckMessage
	collectorOptions.GetMessages = nsqCollector.GetMessages
	collectorOptions.PublishMessage = nsqCollector.PublishMessage
	collectorOptions.Wake = nsqCollector.Wake
	collectorOptions.Sleep = nsqCollector.Sleep

	nsqCollector.BaseCollector = collectors.CreateBaseCollector(collectorOptions)

	nsqCollector.scale = 1

	return nsqCollector, nil
}

//Sleep -
func (collector *NsqCollector) Sleep() {
	collector.Sleeping = true
	collector.Consumer.ChangeMaxInFlight(0)
}

//Wake -
func (collector *NsqCollector) Wake() {
	collector.Sleeping = false
	collector.Consumer.ChangeMaxInFlight(collector.scale)
}

func (collector *NsqCollector) Scale(numCollectors int) bool {
	collector.scale = numCollectors
	collector.Consumer.ChangeMaxInFlight(collector.scale)
	return true
}

//HandleMessage - Received messages from NSQ one at a time.
func (collector *NsqCollector) HandleMessage(message *nsq.Message) error {
	result := collector.BusinessProcessor(message.Body)
	if result.Err != nil {
		msg := collectors.MessageWrapper{
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

//GetMessages - Not used for the NSQ Collector since it has a callback above (HandleMessage)
//              and handles it's own exponential backoff as well.
func (collector *NsqCollector) GetMessages() ([]collectors.MessageWrapper, error) {
	messages := make([]collectors.MessageWrapper, 0)

	return messages, nil
}

//PublishMessage -
func (collector *NsqCollector) PublishMessage(message *collectors.MessageWrapper, _ int64, errFlag bool) error {

	if errFlag {
		return collector.Producer.Publish(collector.ErrorTopic, message.MessageBody)
	}
	message.Message.(*nsq.Message).RequeueWithoutBackoff(time.Duration(collector.RetryIntervalSecs) * time.Second)
	return nil
}

//AckMessage - Not needed for NSQ (Messages are not acknowledged
func (collector *NsqCollector) AckMessage(message collectors.MessageWrapper) error {
	return nil
}

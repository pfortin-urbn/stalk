package google_pubsub

import (
	"cloud.google.com/go/pubsub"

	"stalk/collectors"
)

type PubSubCollector struct {
	*collectors.BaseCollector
}

func CreatePubSubCollector(collectorOptions collectors.CollectorOptions) (*PubSubCollector, error) {
	return &PubSubCollector{}, nil
}

func (collector *PubSubCollector) Sleep() {

}

func (collector *PubSubCollector) Wake() {

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

func (collector *PubSubCollector) GetMessages() ([]collectors.MessageWrapper, error) {
	var err error
	messages := make([]collectors.MessageWrapper, 0)

	return messages, err
}

func (collector *PubSubCollector) PublishMessage(message *collectors.MessageWrapper, delaySeconds int64, errFlag bool) error {
	var err error
	return err
}

func (collector *PubSubCollector) AckMessage(receiptHandle string) error {
	var err error
	return err
}

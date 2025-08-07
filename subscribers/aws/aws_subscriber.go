package aws

import (
	"fmt"
	"github.com/pfortin-urbn/stalk/subscribers"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Test with GoAWS!!!
var Endpoint = os.Getenv("AWS_ENDPOINT") //SQS Proxy?

type SqsSubscriber struct {
	*subscribers.BaseSubscriber
	SqsClient *sqs.SQS
	scale     int
}

var sqsCollectors = make([]*SqsSubscriber, 0)

func CreateSqsSubscriber(collectorOptions subscribers.SubscriberOptions) (*SqsSubscriber, error) {
	awsConfig := &aws.Config{
		Region: aws.String(collectorOptions.Region),
	}

	if Endpoint != "" { //To use alternative source like goaws or a proxy
		awsConfig.Endpoint = &Endpoint
	}

	session, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, err
	}
	client := sqs.New(session, awsConfig)

	response, err := client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName:              &collectorOptions.SourceTopic,
		QueueOwnerAWSAccountId: &collectorOptions.AccountID,
	})
	if err != nil {
		return nil, err
	}
	collectorOptions.SourceTopic = *response.QueueUrl //Replace the QueueName with the Queue URL

	response, err = client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName:              &collectorOptions.ErrorTopic,
		QueueOwnerAWSAccountId: &collectorOptions.AccountID,
	})
	if err != nil {
		return nil, err
	}
	collectorOptions.ErrorTopic = *response.QueueUrl //Replace the QueueName with the Queue URL

	sc := &SqsSubscriber{}
	collectorOptions.AckMessage = sc.AckMessage
	collectorOptions.GetMessages = sc.GetMessages
	collectorOptions.PublishMessage = sc.PublishMessage
	collectorOptions.Sleep = sc.Sleep
	collectorOptions.Wake = sc.Wake

	sc.BaseSubscriber = subscribers.CreateBaseCollector(collectorOptions)
	sc.SqsClient = client

	sc.scale = 1

	return sc, nil
}

func Scale(numCollectors int) bool {
	newScale := numCollectors - len(sqsCollectors)
	for x := newScale; x != 0; {
		if newScale > 0 {
			//remove a collector
			x--
		} else {
			//add a collector
			x--
		}
	}
	return true
}

func (collector *SqsSubscriber) Sleep() {
	collector.Sleeping = true
}

func (collector *SqsSubscriber) Wake() {
	collector.Sleeping = false
	go collector.timeout()
}

func (collector *SqsSubscriber) timeout() {
	time.Sleep(time.Duration(collector.PollingPeriod) * time.Millisecond)
	collector.ChannelToInitiatePollRequest <- true
}

// AWS SQS client requires you to pool for messages so the pool must
// be aware of how to call the Library Methods for exponential backoff
// and Process Message Results from the business logic
func (collector *SqsSubscriber) PollForMessages() {
	go collector.timeout()
	for {
		select {
		case <-collector.ChannelToInitiatePollRequest:
			if !collector.Sleeping {
				fmt.Println("Processing messages...")
				msgs, err := collector.GetMessages()
				collector.ProcessExponentialBackoff(err)
				for _, msg := range msgs {
					result := collector.BusinessProcessor(msg.MessageBody) //Business logic
					collector.ProcessMessageResult(msg, result)
				}
			}
			go collector.timeout()
		case <-collector.ChannelDone:
			return
		}
	}
}

func (collector *SqsSubscriber) GetMessages() ([]subscribers.MessageWrapper, error) {
	var waitTimeSecs int64 = 10
	attributeName := "Retries"
	params := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages:   aws.Int64(10),
		MessageAttributeNames: []*string{&attributeName},
		QueueUrl:              aws.String(collector.SourceTopic), // Required
		WaitTimeSeconds:       &waitTimeSecs,
	}
	resp, err := collector.SqsClient.ReceiveMessage(params)
	messages := make([]subscribers.MessageWrapper, 0)
	if err == nil {
		awsMessages := resp.Messages
		for _, awsMessage := range awsMessages {
			retries, _ := strconv.Atoi(*awsMessage.MessageAttributes["Retries"].StringValue)
			messages = append(messages, subscribers.MessageWrapper{
				MessageBody:   []byte(*awsMessage.Body),
				ReceiptHandle: *awsMessage.ReceiptHandle,
				Retries:       retries,
				Retry:         false,
				Fatal:         false,
			})
		}
	}
	return messages, err
}

func (collector *SqsSubscriber) PublishMessage(message *subscribers.MessageWrapper, delaySeconds int64, errFlag bool) error {
	qUrl := collector.SourceTopic
	if errFlag {
		qUrl = collector.ErrorTopic
	}
	sendMessageInput := &sqs.SendMessageInput{
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"Retries": &sqs.MessageAttributeValue{
				DataType:    aws.String("Number"),
				StringValue: aws.String(fmt.Sprintf("%d", message.Retries)),
			},
		},
		MessageBody:  aws.String(string(message.MessageBody)),
		QueueUrl:     aws.String(qUrl),
		DelaySeconds: &delaySeconds,
	}
	_, err := collector.SqsClient.SendMessage(sendMessageInput)
	return err
}

func (collector *SqsSubscriber) AckMessage(message subscribers.MessageWrapper) error {
	delParams := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(collector.SourceTopic), // Required
		ReceiptHandle: aws.String(message.ReceiptHandle), // Required
	}
	_, err := collector.SqsClient.DeleteMessage(delParams)
	return err
}

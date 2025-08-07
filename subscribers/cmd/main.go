package main

import (
	"log"

	"github.com/pfortin-urbn/stalk/subscribers"
	"github.com/pfortin-urbn/stalk/subscribers/aws"
	"github.com/pfortin-urbn/stalk/subscribers/google_pubsub"
	"github.com/pfortin-urbn/stalk/subscribers/nsq"
)

func businessLogic(msg []byte) *subscribers.Result {
	log.Printf("Business Logic - %s\n", string(msg))
	return &subscribers.Result{
		Err:   nil,
		Retry: false,
		Fatal: false,
	}
}

// https://sqs.us-east-1.amazonaws.com/794373491471/input
// https://sqs.us-east-1.amazonaws.com/478989820108/PAUL_TEST
func RunAwsCollector() {
	var sc *aws.SqsSubscriber
	var err error
	var options = subscribers.SubscriberOptions{
		Region:            "us-east-1",
		AccountID:         "794373491471",
		PollingPeriod:     10,
		MaxPollingPeriod:  60,
		MaxRetries:        3,
		RetryIntervalSecs: 60,
		SourceTopic:       "input",
		ErrorTopic:        "error",
		ApiPort:           8080,
		BusinessProcessor: businessLogic,
		GetMessages:       nil,
		PublishMessage:    nil,
		AckMessage:        nil,
	}
	sc, err = aws.CreateSqsSubscriber(options)
	if err != nil {
		panic(err)
	}
	sc.Wake()

}

func RunNsqSubscriber() {
	var sc *nsq.NsqSubscriber
	var err error
	var options = subscribers.SubscriberOptions{
		PollingPeriod:     10,
		MaxPollingPeriod:  60,
		MaxRetries:        3,
		RetryIntervalSecs: 10,
		SourceTopic:       "inputs",
		ErrorTopic:        "errors",
		BusinessProcessor: businessLogic,
	}
	sc, err = nsq.CreateNsqSubscriber(options)
	if err != nil {
		panic(err)
	}
	sc.Wake()
}

// TODO - Add retry logic for connection timeouts (publish)
func RunGooglePubSubSubscriber() {
	var sc *google_pubsub.PubSubSubScriber
	var err error
	var options = subscribers.SubscriberOptions{
		//Google Project Id
		AccountID:          "contrail-6d68d",
		PollingLimit:       5,
		PollingPeriod:      5,
		MaxRetries:         3,
		RetryIntervalSecs:  20,
		SourceTopic:        "STALK-SOURCE-TOPIC",
		SourceSubscription: "STALK-SOURCE-SUBSCRIPTION",
		ErrorTopic:         "STALK-ERROR-TOPIC",
		BusinessProcessor:  businessLogic,
	}
	sc, err = google_pubsub.CreatePubSubSubscriber(options)
	if err != nil {
		panic(err)
	}

	sc.Wake()
}

func main() {
	RunGooglePubSubSubscriber()

	waitCh := make(chan bool)
	<-waitCh
}

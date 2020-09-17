package main

import (
	"log"

	"github.com/pfortin-urbn/stalk/collectors"
	"github.com/pfortin-urbn/stalk/collectors/aws"
	"github.com/pfortin-urbn/stalk/collectors/google_pubsub"
	"github.com/pfortin-urbn/stalk/collectors/nsq"
)

func businessLogic(msg []byte) *collectors.Result {
	log.Printf("Business Logic - %s\n", string(msg))
	return &collectors.Result{
		Err:   nil,
		Retry: false,
		Fatal: false,
	}
}

//https://sqs.us-east-1.amazonaws.com/794373491471/input
//https://sqs.us-east-1.amazonaws.com/478989820108/PAUL_TEST
func RunAwsCollector() {
	var sc *aws.SqsCollector
	var err error
	var options = collectors.CollectorOptions{
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
	sc, err = aws.CreateSqsCollector(options)
	if err != nil {
		panic(err)
	}
	sc.Wake()

}

func RunNsqCollector() {
	var sc *nsq.NsqCollector
	var err error
	var options = collectors.CollectorOptions{
		PollingPeriod:     10,
		MaxPollingPeriod:  60,
		MaxRetries:        3,
		RetryIntervalSecs: 10,
		SourceTopic:       "inputs",
		ErrorTopic:        "errors",
		BusinessProcessor: businessLogic,
	}
	sc, err = nsq.CreateNsqCollector(options)
	if err != nil {
		panic(err)
	}
	sc.Wake()
}

// TODO - Add retry logic for connection timeouts (publish)
func RunGooglePubSubCollector() {
	var sc *google_pubsub.PubSubCollector
	var err error
	var options = collectors.CollectorOptions{
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
	sc, err = google_pubsub.CreatePubSubCollector(options)
	if err != nil {
		panic(err)
	}

	sc.Wake()
}

func main() {
	RunGooglePubSubCollector()

	waitCh := make(chan bool)
	<-waitCh
}

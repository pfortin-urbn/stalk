package main

import (
	"errors"
	"fmt"
	"github.com/pfortin-urbn/stalk/collectors"
	"github.com/pfortin-urbn/stalk/collectors/aws"
	"github.com/pfortin-urbn/stalk/collectors/google_pubsub"
	"github.com/pfortin-urbn/stalk/collectors/nsq"
	"time"
)

func businessLogic(msg []byte) *collectors.Result {
	fmt.Printf("Business Logic - %s\n", string(msg))
	return &collectors.Result{
		Err:   errors.New("boom"),
		Retry: true,
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

// TODO - Add retry logic
//  Look for connection timeouts (publish)
//  Make sure sleep and wake works (set up recurring publish on cloud or something)
func RunGooglePubSubCollector() {
	var sc *google_pubsub.PubSubCollector
	var err error
	var options = collectors.CollectorOptions{
		//Google Project Id
		AccountID:          "stalk-1599139247342",
		PollingLimit:       1,
		PollingPeriod:      10,
		MaxPollingPeriod:   60,
		MaxRetries:         3,
		RetryIntervalSecs:  60,
		SourceTopic:        "STALK-SOURCE-TOPIC",
		SourceSubscription: "STALK-SOURCE-SUBSCRIPTION",
		ErrorTopic:         "STALK-TARGET-TOPIC",
		BusinessProcessor:  businessLogic,
	}
	sc, err = google_pubsub.CreatePubSubCollector(options)
	if err != nil {
		panic(err)
	}
	messageBody := `{
			"profileId": "2ed3920f-8f8f-44d3-ae6b-2bced9572a6a",
			"cartId": "ba876272-9591-4594-9423-21c99a85da5a",
			"dataCenterId": "US-PA",
			"draft": false,
			"generateOrder": true,
			"timestamp": 1564163629.6154637,
			"publishOrder": true
		}`
	msg := collectors.MessageWrapper{
		MessageBody: []byte(messageBody),
		Retries:     1,
		Retry:       false,
		Fatal:       true,
	}

	err = sc.PublishMessage(&msg, 0, false)
	if err != nil {
		fmt.Println(err)
	}

	time.Sleep(time.Duration(15) * time.Second)
	sc.Sleep()
	time.Sleep(time.Duration(5) * time.Second)

	sc.Wake()
}

func main() {
	RunGooglePubSubCollector()

	waitCh := make(chan bool)
	<-waitCh
}

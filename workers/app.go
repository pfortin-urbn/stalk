package main

import (
	"errors"
	"fmt"

	"stalk/collectors"
	"stalk/collectors/aws"
	"stalk/collectors/nsq"
)

func businessLogic(msg []byte) *collectors.Result {
	fmt.Println(string(msg))
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
		AccountID:         "478989820108",
		PollingPeriod:     10,
		MaxPollingPeriod:  60,
		MaxRetries:        3,
		RetryIntervalSecs: 60,
		SourceTopic:       "PAUL_TEST",
		ErrorTopic:        "PAUL_TILES",
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

func main() {
	RunNsqCollector()

	waitCh := make(chan bool)
	<-waitCh
}

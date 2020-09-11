# Stalk
## The best part of the celery!!!  (who eats the leaves anyway)

Generalized async message queue processing.  You Stalk app contains two parts the Collector, 
which is provided for you, and the business logic, you provide this. `workers/app.go` shows a toy example.

your business logic entry point simple has to abide by the simple interface:
 func([]byte) *collectors.Result
 
 In the result there are 3 fields to supply, any errors encountered, and 
 the retry and fatal falgs to tell stalk if you want a retry or this is a critical error
 and stalk should just put the message on the error topic/queue.


`CollectorOptions` for the call to create the BaseCollector:
- PollingPeriod     int       How often to poll for polling message systems (SQS)
- MaxPollingPeriod  int       During backoff/retry max time between polls
- MaxRetries        int       For collector backoff/retry
- RetryIntervalSecs int64     # of secs between retries
- SourceTopic       string    Topic/Queue name where the messages will arrive.
- ErrorTopic        string    Topic/Queue name where to put messages after they exhaust all reties or  
                              the business logic dictates it.
- Region            string    For AWS
- AccountID         string    For AWS
- BusinessProcessor func([]byte) *Result
                              Application's business logic
- GetMessages       func() ([]MessageWrapper, error)
                              Message polling method.
- PublishMessage    func(message *MessageWrapper, delaySeconds int64, errFlag bool) error
                              Method to publish messages to a topic/queue 
- AckMessage        func(receiptHandle string) error  
                              Method to Ack messages
- Sleep             func()    Stop receiving messages from the message broker 
                              (all Collectors start in paused mode)
- Wake              func()    Start receiving messages from the message broker


---

If your message processing system is not included you can add one and all PRs to add any 
message queue are welcome.

- To create a new message collector, you need to implement 5 methods:
	- **CreateXYZCollector**(CollectorOptions) (*Collector, error)
	    - Make sure you extend the BaseCollector and pass in the required `collectorOptions`.
		- Create and return your collector making sure all required go routines are started (i.e.: AWS/SQS)
	- **Sleep**()
		- Should stop collector from polling or stop the sending of messages to the handler(s)
	- **Wake**() 
	- **GetMessages**() ([]collectors.MessageWrapper, error)
		- For services that require to be polled (AWS/SQS, Pubsub, etc...) this function should return an array of messages
		- Messaging systems that have handlers (i..e.: NSQ), this can return an empty array.
	- **PublishMessage**(message *collectors.MessageWrapper, _ int64, errFlag bool) error
	- **AckMessage**(receiptHandle string) error
		- if no Acks are required like NSQ return nil is fine.

# Stalk
## The best part of the celery!!!

Generalized async message queue processing.  You Stalk app contains two parts the Collector, 
which is provided for you, and the business logic, you provide this. `workers/app.go` shows a toy example.


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

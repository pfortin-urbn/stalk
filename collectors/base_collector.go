package collectors

import (
	"log"
	"math"
	"net/http"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
)

type BaseCollector struct {
	Collector
	CollectorType                string
	ChannelToInitiatePollRequest chan bool
	ChannelDone                  chan bool
	Retrying                     bool
	Retries                      int
	Sleeping                     bool
	earlyAck                     bool
	PollingPeriod                int
	MaxPollingPeriod             int
	MaxRetries                   int
	RetryIntervalSecs            int64
	SourceTopic                  string
	ErrorTopic                   string
	Sleep                        func()
	Wake                         func()
	BusinessProcessor            func([]byte) *Result
	GetMessages                  func() ([]MessageWrapper, error)
	PublishMessage               func(message *MessageWrapper, delaySeconds int64, errFlag bool) error
	AckMessage                   func(receiptHandle string) error
}

func CreateBaseCollector(collectorOptions CollectorOptions) *BaseCollector {
	bc := &BaseCollector{
		ChannelToInitiatePollRequest: make(chan bool),
		ChannelDone:                  make(chan bool),
		Retrying:                     false,
		Retries:                      0,
		Sleeping:                     false,
		earlyAck:                     false,
		PollingPeriod:                collectorOptions.PollingPeriod,
		MaxPollingPeriod:             collectorOptions.MaxPollingPeriod,
		MaxRetries:                   collectorOptions.MaxRetries,
		RetryIntervalSecs:            collectorOptions.RetryIntervalSecs,
		SourceTopic:                  collectorOptions.SourceTopic,
		ErrorTopic:                   collectorOptions.ErrorTopic,
		BusinessProcessor:            collectorOptions.BusinessProcessor,
		GetMessages:                  collectorOptions.GetMessages,
		PublishMessage:               collectorOptions.PublishMessage,
		AckMessage:                   collectorOptions.AckMessage,
		Sleep:                        collectorOptions.Sleep,
		Wake:                         collectorOptions.Wake,
	}
	//Start server for commands
	go bc.collectorApi()

	return bc
}

func (collector *BaseCollector) ProcessExponentialBackoff(err error) {
	if err != nil {
		if collector.Retrying == false {
			log.Printf("Detected AWS is not available for queue %s", collector.SourceTopic)
		}
		collector.Retrying = true
		collector.Retries = collector.Retries + 1
		collector.PollingPeriod = int(math.Pow(2, float64(collector.Retries)))
		if collector.PollingPeriod > collector.MaxPollingPeriod {
			collector.PollingPeriod = collector.MaxPollingPeriod
		}
		log.Printf("Error trying to receive messages from queue: %s, error: %s", collector.SourceTopic, err.Error())
	} else {
		if collector.Retrying {
			log.Printf("Detected AWS is back online for queue %s", collector.SourceTopic)
			collector.Retrying = false
			collector.Retries = 0
			collector.PollingPeriod = 0
		}
	}
}

//processMessageResult - Workers requests processing
func (collector *BaseCollector) ProcessMessageResult(msg MessageWrapper, result *Result) {
	defer collector.AckMessage(msg.ReceiptHandle)

	message := &MessageWrapper{
		MessageBody: msg.MessageBody,
		Retries:     msg.Retries,
		Retry:       msg.Retry,
		Fatal:       msg.Fatal,
		Message:     msg.Message,
	}

	if result.Fatal || (result.Err != nil && result.Retry == true && msg.Retries >= collector.MaxRetries) {
		//Publish to Error Queue
		collector.PublishMessage(message, 0, true)
		return
	}
	if result.Err != nil && result.Retry {
		//Publish to Source Queue
		message.Retries++
		collector.PublishMessage(message, collector.RetryIntervalSecs, false)
	}
}

func (collector *BaseCollector) collectorApi() {
	// Echo instance
	e := echo.New()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Route => handler
	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]interface{}{
			"sleeping": collector.Sleeping,
		})
	})
	e.GET("/sleep", func(c echo.Context) error {
		collector.Sleep()
		return c.JSON(http.StatusOK, map[string]interface{}{
			"sleeping": collector.Sleeping,
		})
	})
	e.GET("/wake", func(c echo.Context) error {
		collector.Wake()
		return c.JSON(http.StatusOK, map[string]interface{}{
			"sleeping": collector.Sleeping,
		})
	})
	// Start server
	e.Logger.Fatal(e.Start(":1323"))
}

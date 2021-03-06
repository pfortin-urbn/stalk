package collectors

import (
	"fmt"
	"log"
	"math"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/prometheus/client_golang/prometheus"
	uuid "github.com/satori/go.uuid"
)

type BaseCollector struct {
	Collector
	CollectorId                  string
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
	ApiPort                      int
	Sleep                        func()
	Wake                         func()
	BusinessProcessor            func([]byte) *Result
	GetMessages                  func() ([]MessageWrapper, error)
	PublishMessage               func(message *MessageWrapper, delaySeconds int64, errFlag bool) error
	AckMessage                   func(message MessageWrapper) error

	/* Stats */
	MessagesProcessed prometheus.Counter
	MessagesRetried   prometheus.Counter
	MessagesFailed    prometheus.Counter
}

func CreateBaseCollector(collectorOptions CollectorOptions) *BaseCollector {
	collectorId := collectorOptions.CollectorId
	if collectorId == "" {
		collectorId = uuid.NewV4().String()
	}
	bc := &BaseCollector{
		CollectorId:                  collectorId,
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
		ApiPort:                      collectorOptions.ApiPort,
		BusinessProcessor:            collectorOptions.BusinessProcessor,
		GetMessages:                  collectorOptions.GetMessages,
		PublishMessage:               collectorOptions.PublishMessage,
		AckMessage:                   collectorOptions.AckMessage,
		Sleep:                        collectorOptions.Sleep,
		Wake:                         collectorOptions.Wake,
	}

	bc.MessagesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "messages_received",
		Help: "Number of messages processed.",
	})
	prometheus.MustRegister(bc.MessagesProcessed)
	bc.MessagesRetried = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "messages_retried",
		Help: "Number of messages with errors that were retried.",
	})
	prometheus.MustRegister(bc.MessagesRetried)
	bc.MessagesFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "messages_failed",
		Help: "Number of messages placed on error queue.",
	})
	prometheus.MustRegister(bc.MessagesFailed)

	//Start server for commands
	go bc.collectorApi()

	return bc
}

func (collector *BaseCollector) ProcessExponentialBackoff(err error) {
	if err != nil {
		if collector.Retrying == false {
			log.Printf("Detected queue service is not available for queue %s", collector.SourceTopic)
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
			log.Printf("Detected queue service is back online for queue %s", collector.SourceTopic)
			collector.Retrying = false
			collector.Retries = 0
			collector.PollingPeriod = 0
		}
	}
}

//processMessageResult - Workers requests processing
func (collector *BaseCollector) ProcessMessageResult(msg MessageWrapper, result *Result) {
	defer collector.AckMessage(msg)
	collector.MessagesProcessed.Add(1)

	if result.Fatal || (result.Err != nil && result.Retry == true && msg.Retries >= collector.MaxRetries-1) {
		//Publish to Error Queue
		collector.MessagesFailed.Add(1)
		collector.PublishMessage(&msg, 0, true)
		return
	}
	if result.Err != nil && result.Retry {
		//Publish to Source Queue
		collector.MessagesRetried.Add(1)
		msg.Retries++
		collector.PublishMessage(&msg, collector.RetryIntervalSecs, false)
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
			"collector_id": collector.CollectorId,
			"sleeping":     collector.Sleeping,
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
	e.GET("/metrics", func(c echo.Context) error {
		w := c.Response().Writer
		r := c.Request()
		promhttp.Handler().ServeHTTP(w, r)
		return nil
	})
	// Start server
	apiPort := collector.ApiPort
	if apiPort == 0 {
		apiPort = 1323
	}
	e.Logger.Fatal(e.Start(fmt.Sprintf(":%d", apiPort)))
}

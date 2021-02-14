package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
)

func pullMsgs(projectID, subID string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %v", err)
	}
	topic := client.Topic("STALK-SOURCE-TOPIC")

	// Consume 10 messages.
	var mu sync.Mutex
	//received := 0
	sub, err := client.CreateSubscription(context.Background(), "gotestme", pubsub.SubscriptionConfig{
		Topic:            topic,
		PushConfig:       pubsub.PushConfig{},
		AckDeadline:      10 * time.Second,
		ExpirationPolicy: nil,
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     "projects/contrail-6d68d/topics/STALK-ERROR-TOPIC",
			MaxDeliveryAttempts: 5,
		},
		RetryPolicy: &pubsub.RetryPolicy{
			MinimumBackoff: time.Duration(10 * time.Second),
			MaximumBackoff: time.Duration(10 * time.Second),
		},
	})
	if err != nil {
		sub = client.Subscription(subID)
		//return fmt.Errorf("pubsub.NewSubscription: %v", err)
	}

	//sub := client.Subscription(subID)
	log.Printf("%+v\n", sub.ReceiveSettings)
	cfg, err := sub.Config(context.Background())
	log.Printf("%+v\n%+v\n+%v\n", cfg, cfg.DeadLetterPolicy, cfg.DeadLetterPolicy)
	cctx, _ := context.WithCancel(ctx)
	err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()
		log.Printf("Got message: %q(%d)\n", string(msg.Data), *msg.DeliveryAttempt)
		if *msg.DeliveryAttempt == 5 {
			msg.Ack()
		} else {
			msg.Nack()
		}
	})
	if err != nil {
		return fmt.Errorf("Receive: %v", err)
	}
	return nil
}

func main() {
	fmt.Println(pullMsgs("contrail-6d68d", "STALK-SOURCE-SUBSCRIPTION"))
}

package main

import (
	"context"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"log"
	"net/url"
)

func initBus(ctx context.Context, mqttHost, topic, clientId string, f func(pr paho.PublishReceived) (bool, error)) (*autopaho.ConnectionManager, error) {
	u, err := url.Parse(mqttHost)
	if err != nil {
		log.Fatalf("URL parsing failed: %v", err)
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     20,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         60,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			log.Println("MQTT connection %s starting up", mqttHost)
			if _, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}}}); err != nil {
				log.Fatalf("Failed to subscribe to %s: %v", topic, err)
			}
			log.Println("MQTT subscription on host %s for topic %s successfully", mqttHost, topic)
		},
		OnConnectError: func(err error) { log.Fatalf("Connection error: %v", err) },
		ClientConfig: paho.ClientConfig{
			ClientID:          clientId,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){f},
			OnClientError:     func(err error) { log.Fatalf("Client error: %v", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				log.Fatalf("Server disconnect: %v", d.ReasonCode)
			},
		},
	}

	c, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		log.Fatalf("Connection creation failed: %v", err)
	}
	if err = c.AwaitConnection(ctx); err != nil {
		log.Fatalf("Awaiting connection failed: %v", err)
	}
	log.Println("MQTT subscription successful")
	return c, nil
}

func publish(ctx context.Context, topic, message string, c *autopaho.ConnectionManager) {
	if _, err := c.Publish(ctx, &paho.Publish{QoS: 1, Topic: topic, Payload: []byte(message)}); err != nil && ctx.Err() == nil {
		log.Fatalf("Publish failed: %v", err)
	}
}

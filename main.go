package main

import (
	"context"
	"crypto/tls"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

type kafkaBrokerList []string

func (k *kafkaBrokerList) String() string {
	return strings.Join(*k, ",")
}

func (k *kafkaBrokerList) Set(value string) error {
	*k = strings.Split(value, ",")
	return nil
}

var (
	kafkaBrokers    kafkaBrokerList
	KafkaTopic      string
	region          string
	accessKeyID     string
	secretAccessKey string
	staticToken     string
	createTopic     bool
)

func init() {
	flag.Var(&kafkaBrokers, "brokers", "Kafka brokers to connect to")
	flag.StringVar(&KafkaTopic, "topic", "", "Kafka topic to produce messages to")
	flag.StringVar(&region, "region", "", "AWS region")
	flag.StringVar(&accessKeyID, "access-key-id", "", "AWS access key ID")
	flag.StringVar(&secretAccessKey, "secret-access-key", "", "AWS secret access key")
	flag.StringVar(&staticToken, "static-token", "", "Static token for authentication")
	flag.BoolVar(&createTopic, "create-topic", false, "Create topic")
}

func validateFlags() {
	if len(kafkaBrokers) == 0 {
		log.Fatal("No Kafka brokers provided")
	}
	if KafkaTopic == "" {
		log.Fatal("No Kafka topic provided")
	}
	if staticToken == "" {
		if region == "" {
			log.Fatal("No AWS region provided")
		}
		if accessKeyID == "" {
			log.Fatal("No AWS access key ID provided")
		}
		if secretAccessKey == "" {
			log.Fatal("No AWS secret access key provided")
		}
	}
}

type MSKAccessToken struct {
	AccessToken string
}

func (m *MSKAccessToken) Token() (*sarama.AccessToken, error) {
	return &sarama.AccessToken{Token: m.AccessToken}, nil
}

type MSKAccessTokenProvider struct {
}

func (m *MSKAccessTokenProvider) Token() (*sarama.AccessToken, error) {
	token, expirationTimeMs, err := signer.GenerateAuthTokenFromCredentialsProvider(
		context.TODO(), region,
		credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, ""))
	log.Printf("Token generated: %s, %s", token, time.Duration(expirationTimeMs)*time.Millisecond)
	return &sarama.AccessToken{Token: token}, err
}

func main() {
	flag.Parse()

	validateFlags()

	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	client, err := newClient()
	if err != nil {
		panic(err)
	} else {
		log.Println("Kafka AsyncProducer up and running!")
	}

	if createTopic {
		admin, err := sarama.NewClusterAdminFromClient(client)
		if err != nil {
			log.Fatalf("Failed to create cluster admin: %v", err)
		}
		err = admin.CreateTopic(KafkaTopic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil {
			log.Fatalf("Failed to create topic: %v", err)
		}
		log.Printf("Topic %s created successfully\n", KafkaTopic)
	} else {
		// Trap SIGINT to trigger a graceful shutdown.
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)

		err = client.RefreshMetadata(KafkaTopic)
		if err != nil {
			log.Fatalf("Failed to refresh metadata: %v", err)
		}
		log.Printf("Refreshed metadata for topic %s\n", KafkaTopic)

		brokers := client.Brokers()
		for _, broker := range brokers {
			log.Printf("Broker: %s\n", broker.Addr())
			ok, err := broker.Connected()
			if err != nil {
				log.Printf("Error checking connection: %v\n", err)
			}
			if ok {
				log.Printf("Broker %s is connected\n", broker.Addr())
			} else {
				log.Printf("Broker %s is not connected\n", broker.Addr())
			}
		}
	}
}

func newClient() (sarama.Client, error) {
	// Set the SASL/OAUTHBEARER configuration
	config := sarama.NewConfig()
	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	if staticToken != "" {
		config.Net.SASL.TokenProvider = &MSKAccessToken{
			AccessToken: staticToken,
		}
	} else {
		config.Net.SASL.TokenProvider = &MSKAccessTokenProvider{}
	}

	// Set the TLS configuration
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{}

	return sarama.NewClient(kafkaBrokers, config)
}

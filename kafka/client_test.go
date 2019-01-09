package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
)

func Test_ClientAPIVersion(t *testing.T) {
	// Default to 0
	client := &Client{}
	maxVersion := client.versionForKey(32, 1)
	if maxVersion != 0 {
		t.Errorf("Got %d, expected %d", maxVersion, 0)
	}

	// use the max version the broker supports, if it's less than the requested
	// version
	client.supportedAPIs = map[int]int{32: 0}
	maxVersion = client.versionForKey(32, 1)
	if maxVersion != 0 {
		t.Errorf("Got %d, expected %d", maxVersion, 0)
	}

	// while the broker supports 2, terraform-provider-kafka only supports 1, so
	// use that
	client.supportedAPIs = map[int]int{32: 2}
	maxVersion = client.versionForKey(32, 1)
	if maxVersion != 1 {
		t.Errorf("Got %d, expected %d", maxVersion, 1)
	}
}

func Test_kafkaConfigVersion(t *testing.T) {
	c := Config{}

	cfg, err := c.newKafkaConfig()
	if err != nil {
		t.Fatalf("Config should be valid: %s", err)
	}

	if cfg.Version != sarama.V1_0_0_0 {
		t.Errorf("Default version should be v1; got %s", cfg.Version)
	}
}

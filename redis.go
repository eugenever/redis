package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/redis/rueidis"
)

var (
	TimeoutCheckRedisInstances    = 2 // seconds
	TimeoutCheckRefusedConnection = 1 // seconds
	ErrRedisPoolIsUnavailable     = errors.New("all Redis instances from the pool are unavailable")
)

// NewClient create new rueidis client.
// password parameter allow to be empty.
func NewClient(host string, port int, password string) (rueidis.Client, error) {
	redisUrl := fmt.Sprintf("%s:%d", host, port)
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{redisUrl},
		Password:    password,
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

func TestEnvs() (string, string, string) {
	return "TEST_REDIS_HOST", "TEST_REDIS_PORT", "TEST_REDIS_PASSWORD"
}

func NewClientFromEnv(hostEnv string, portEnv string, passwordEnv string) (rueidis.Client, error) {
	redisHost := os.Getenv(hostEnv)
	if redisHost == "" {
		fmt.Printf("environment variable '%s' must be defined", hostEnv)
		os.Exit(1)
	}
	redisPort := os.Getenv(portEnv)
	if redisPort == "" {
		fmt.Printf("environment variable '%s' must be defined", portEnv)
		os.Exit(1)
	}
	port, err := strconv.Atoi(redisPort)
	if err != nil {
		return nil, err
	}
	redisPassword := os.Getenv(passwordEnv)
	return NewClient(redisHost, port, redisPassword)
}

func NewRedisClient(host string, port int, password *string) (rueidis.Client, error) {
	redisUrl := fmt.Sprintf("%s:%d", host, port)
	var p string
	if password != nil {
		p = *password
	}
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{redisUrl},
		Password:    p,
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

func NewRedisCluster(initAddress []string) (rueidis.Client, error) {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: initAddress,
		ShuffleInit: true,
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

func InitConsumerGroup(
	ctx context.Context,
	client rueidis.Client,
	streamName,
	groupName,
	consumerName string,
) error {
	err := XgroupCreate(ctx, client, streamName, groupName)
	if err != nil {
		if !IsBusyGroup(err) {
			return err
		}
	}
	err = XgroupDelconsumer(client, streamName, groupName, consumerName)
	if err != nil {
		return err
	}
	err = XgroupCreateconsumer(client, streamName, groupName, consumerName)
	if err != nil {
		return err
	}
	return nil
}

func XgroupCreate(
	ctx context.Context,
	client rueidis.Client,
	streamName,
	groupName string,
) error {
	err := client.Do(
		ctx,
		client.
			B().
			XgroupCreate().
			Key(streamName).
			Group(groupName).
			Id("$").
			Mkstream().
			Build(),
	).Error()
	if err != nil {
		return err
	}
	return nil
}

func XgroupDelconsumer(
	client rueidis.Client,
	streamName,
	groupName,
	consumerName string,
) error {
	ctx := context.Background()
	err := client.Do(
		ctx,
		client.
			B().
			XgroupDelconsumer().
			Key(streamName).
			Group(groupName).
			Consumername(consumerName).
			Build(),
	).Error()
	if err != nil {
		return err
	}
	return nil
}

func XgroupCreateconsumer(
	client rueidis.Client,
	streamName,
	groupName,
	consumerName string,
) error {
	ctx := context.Background()
	err := client.Do(
		ctx,
		client.
			B().
			XgroupCreateconsumer().
			Key(streamName).
			Group(groupName).
			Consumer(consumerName).
			Build(),
	).Error()
	if err != nil {
		return err
	}
	return nil
}

func IsBusyGroup(err error) bool {
	return strings.HasPrefix(err.Error(), "BUSYGROUP")
}

func IsNilMessage(err error) bool {
	return strings.Contains(err.Error(), "nil message") ||
		strings.Contains(err.Error(), "redis: nil")
}

func IsConnectionRefused(err error) bool {
	return strings.Contains(err.Error(), "connection refused")
}

func IsConnectionResetByPeer(err error) bool {
	return strings.Contains(err.Error(), "connection reset by peer")
}

func IsEOF(err error) bool {
	return strings.Contains(err.Error(), "EOF")
}

func IsNogroup(err error) bool {
	return strings.Contains(err.Error(), "NOGROUP")
}

func AnyError(results []rueidis.RedisResult) error {
	for _, result := range results {
		if err := result.Error(); err != nil && !rueidis.IsRedisNil(err) {
			return err
		}
	}
	return nil
}

func IsConnectionError(err error) bool {
	if IsConnectionRefused(err) ||
		IsConnectionResetByPeer(err) ||
		IsEOF(err) ||
		err == rueidis.ErrClosing {
		return true
	}
	return false
}

func ErrorByIndex(results []rueidis.RedisResult, index int) error {
	if index > len(results)-1 || index < 0 {
		return nil
	}
	if err := results[index].Error(); err != nil && !rueidis.IsRedisNil(err) {
		return err
	}
	return nil
}

type Address struct {
	Host string
	Port int
}

func SplitRedisUrl(url string) (*Address, error) {
	parts := strings.Split(url, ":")
	host := parts[0]
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}
	address := &Address{
		Host: host,
		Port: port,
	}
	return address, nil
}

func FlushAllNodes(client rueidis.Client) error {
	ctx := context.Background()
	for _, redisNode := range client.Nodes() {
		err := redisNode.Do(
			ctx,
			redisNode.B().Flushall().Build(),
		).Error()
		if err != nil {
			return err
		}
	}
	return nil
}

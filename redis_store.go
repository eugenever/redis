package redis

import (
	"time"

	"github.com/armon/go-radix"
	"github.com/redis/rueidis"
)

const KeyDiv = ":"

type StoreByKey struct {
	Prefix string
	Ttl    *time.Duration
	Client *rueidis.Client
}

type RedisStore struct {
	ttl    time.Duration
	client rueidis.Client
	byKey  *radix.Tree
}

func NewRedisStore(ttl time.Duration, client rueidis.Client, byKey []StoreByKey) *RedisStore {
	timeToLive := ttl
	if timeToLive > 0 && timeToLive < 1 {
		timeToLive = 1
	}
	radix := radix.New()
	for _, key := range byKey {
		radix.Insert(key.Prefix, key)
	}
	return &RedisStore{ttl: timeToLive, client: client, byKey: radix}
}

func (s *RedisStore) Client(key string) (time.Duration, rueidis.Client) {
	client := s.client
	ttl := s.ttl
	_, raw, ok := s.byKey.LongestPrefix(key)
	if !ok {
		return ttl, client
	}
	byKey, ok := raw.(StoreByKey)
	if !ok {
		return ttl, client
	}
	if byKey.Ttl != nil {
		ttl = *byKey.Ttl
	}
	if byKey.Client != nil {
		client = *byKey.Client
	}
	return ttl, client
}

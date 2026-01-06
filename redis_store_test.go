package redis

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	client, _ := NewRedisClient("localhost", 6379, nil)
	ttl0 := time.Duration(0)
	ttl1 := time.Duration(1)
	ttl2 := time.Duration(2)
	ttl3 := time.Duration(3)
	ttl4 := time.Duration(4)
	ttl5 := time.Duration(5)
	ttl6 := time.Duration(6)
	byKey := []StoreByKey{
		{Prefix: "{cluster:test:index:1}", Client: &client, Ttl: &ttl1},
		{Prefix: "{cluster:test:index:2}", Client: &client, Ttl: &ttl2},
		{Prefix: "{cluster:test:index:3}", Client: &client, Ttl: &ttl3},
		{Prefix: "cluster:test:tile:4", Client: &client, Ttl: &ttl4},
		{Prefix: "cluster:test:tile:5", Client: &client, Ttl: &ttl5},
		{Prefix: "cluster:test:tile:6", Client: &client, Ttl: &ttl6},
	}

	store := NewRedisStore(ttl0, client, byKey)

	ttl, _ := store.Client("{cluster:test:index:1}:id")
	assert.Equal(t, ttl1, ttl)

	ttl, _ = store.Client("{cluster:test:index:2}:time")
	assert.Equal(t, ttl2, ttl)

	ttl, _ = store.Client("{cluster:test:index:3}")
	assert.Equal(t, ttl3, ttl)

	ttl, _ = store.Client("cluster:test:tile:4")
	assert.Equal(t, ttl4, ttl)

	ttl, _ = store.Client("cluster:test:tile:5:6:7")
	assert.Equal(t, ttl5, ttl)

	ttl, _ = store.Client("cluster:test:tile:66")
	assert.Equal(t, ttl6, ttl)
}

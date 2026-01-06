package redis

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

var (
	ErrNoId         = errors.New("id is not defined")
	ErrNoSuchStream = errors.New("no such stream")
)

func CmdExists(ctx context.Context, client rueidis.Client, key string) (bool, error) {
	cmd := client.B().Exists().Key(key).Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		return false, err
	}
	val, err := result.AsInt64()
	if err != nil {
		return false, err
	}
	return val > 0, nil
}

// startId can be $, it is the ID of the last entry in the stream. If set to 0
// it will fetch the entire stream from the beginning
func CmdXgroupCreate(ctx context.Context, client rueidis.Client, stream string, group string, startId string) error {
	cmd := client.B().XgroupCreate().Key(stream).Group(group).Id(startId).Mkstream().Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if !rueidis.IsRedisBusyGroup(err) {
		return err
	}
	return nil
}

func CmdXgroupCreateConsumer(ctx context.Context, client rueidis.Client, stream string, group string, consumer string) error {
	cmd := client.B().XgroupCreateconsumer().Key(stream).Group(group).Consumer(consumer).Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		return err
	}
	_, err = result.AsInt64()
	if err != nil {
		return err
	}
	return nil
}

func CmdXadd(ctx context.Context, client rueidis.Client, stream string, data map[string]string) (string, error) {
	cmd := client.B().Xadd().Key(stream).Id("*").FieldValue().FieldValueIter(func(yield func(string, string) bool) {
		for key, val := range data {
			if !yield(key, val) {
				return
			}
		}
	}).Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		return "", err
	}
	id, err := result.AsBytes()
	if err != nil {
		return "", err
	}
	return string(id), nil
}

// CmdXreadGroup return empty slice when no new elements in the stream.
// Set msgId equal ">" for consuming never delivered messages or "0" for
// getting not yet acknowledged messages.
// Set blockMs equal 0 for non blocking operation.
func CmdXreadGroup(
	ctx context.Context, client rueidis.Client,
	stream string, group string, consumer string,
	msgId string, count int64, blockMs int64,
) ([]rueidis.XRangeEntry, error) {
	// ⬇⬇⬇ From valkey documentation ⬇⬇⬇
	// The special > ID, which means that the consumer want to receive only
	// messages that were never delivered to any other consumer. It just means,
	// give me new messages.
	//
	// Any other ID, that is, 0 or any other valid ID or incomplete ID (just the
	// millisecond time part), will have the effect of returning entries that are
	// pending for the consumer sending the command with IDs greater than the
	// one provided. So basically if the ID is not >, then the command will just
	// let the client access its pending entries: messages delivered to it, but
	// not yet acknowledged. Note that in this case, both BLOCK and NOACK are ignored.
	cmd := xReadGroupCmdGen(client, stream, group, consumer, msgId, count, blockMs)
	result := client.Do(ctx, cmd)
	err := result.NonRedisError()
	if err != nil {
		return nil, err
	}
	err = result.Error()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return []rueidis.XRangeEntry{}, nil
		}
		return nil, err
	}
	xreadResult, err := result.AsXRead()
	if err != nil {
		return nil, err
	}
	streamArr, ok := xreadResult[stream]
	if !ok {
		return nil, fmt.Errorf("stream %s does not exists in result map", stream)
	}
	return streamArr, nil
}

func xReadGroupCmdGen(
	client rueidis.Client, stream string, group string, consumer string, msgId string, count int64, blockMs int64,
) rueidis.Completed {
	if blockMs > 0 {
		return client.B().Xreadgroup().Group(group, consumer).Count(count).Block(blockMs).
			Streams().Key(stream).Id(msgId).Build()
	} else {
		return client.B().Xreadgroup().Group(group, consumer).Count(count).
			Streams().Key(stream).Id(msgId).Build()
	}
}

func CmdXack(ctx context.Context, client rueidis.Client, stream string, group string, id ...string) (int64, error) {
	if len(id) < 1 {
		return 0, ErrNoId
	}
	cmd := client.B().Xack().Key(stream).Group(group).Id(id...).Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		return 0, err
	}
	n, err := result.AsInt64()
	if err != nil {
		return 0, err
	}
	return n, nil
}

var ErrTimeCmdResultLen = errors.New("result of the redis TIME command is not 2 element array")

func CmdTime(ctx context.Context, client rueidis.Client) (time.Time, error) {
	cmd := client.B().Time().Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		return time.Time{}, err
	}
	vals, err := result.AsIntSlice()
	if err != nil {
		return time.Time{}, err
	}
	if len(vals) != 2 {
		return time.Time{}, ErrTimeCmdResultLen
	}
	sec := vals[0]
	nsec := vals[1] * 1000
	resultTime := time.Unix(sec, nsec)
	return resultTime, nil
}

type CmdXinfoGroupsResult struct {
	GroupName       string // group name
	Consumers       int64  // the number of consumers in the group
	Pending         int64  // the length of the group's pending entries list (PEL), which are messages that were delivered but are yet to be acknowledged
	LastDeliveredId string // the ID of the last entry delivered to the group's consumers (not acknowledged)
	EntriesRead     int64  // the logical "read counter" of the last entry delivered to the group's consumers
	Lag             int64  // the number of entries in the stream that are still waiting to be delivered to the group's consumers, or a NULL when that number can't be determined.
}

func CmdXinfoGroups(ctx context.Context, client rueidis.Client, stream string) ([]CmdXinfoGroupsResult, error) {
	cmd := client.B().XinfoGroups().Key(stream).Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		if err.Error() == "no such key" {
			return nil, ErrNoSuchStream
		}
		return nil, err
	}
	resultArr, err := result.ToArray()
	if err != nil {
		return nil, err
	}
	retVal := make([]CmdXinfoGroupsResult, 0, len(resultArr))
	for _, result := range resultArr {
		groupMap, err := result.ToMap()
		if err != nil {
			return nil, err
		}
		info := CmdXinfoGroupsResult{}

		redisMsg := groupMap["name"]
		name, err := redisMsg.AsBytes()
		if err != nil {
			return nil, err
		}
		info.GroupName = string(name)

		redisMsg = groupMap["consumers"]
		consumers, err := redisMsg.ToInt64()
		if err != nil {
			return nil, err
		}
		info.Consumers = consumers

		redisMsg = groupMap["pending"]
		pending, err := redisMsg.ToInt64()
		if err != nil {
			return nil, err
		}
		info.Pending = pending

		redisMsg = groupMap["last-delivered-id"]
		lastDeliveredId, err := redisMsg.ToString()
		if err != nil {
			return nil, err
		}
		info.LastDeliveredId = lastDeliveredId

		redisMsg = groupMap["entries-read"]
		enteriesRead, err := redisMsg.ToInt64()
		if err != nil && !rueidis.IsRedisNil(err) {
			return nil, err
		}
		info.EntriesRead = enteriesRead

		redisMsg = groupMap["lag"]
		lag, err := redisMsg.ToInt64()
		if err != nil {
			return nil, err
		}
		info.Lag = lag

		retVal = append(retVal, info)
	}
	return retVal, nil
}

type CmdXinfoStreamResult struct {
	Length            int64
	RadixTreeKeys     int64
	RadixTreeNodes    int64
	LastGeneratedId   string
	MaxDeletedEntryId string
	EntriesAdded      int64
	Groups            int64
	FirstEntry        rueidis.XRangeEntry
	LastEentry        rueidis.XRangeEntry
}

func CmdXinfoStream(ctx context.Context, client rueidis.Client, stream string) (*CmdXinfoStreamResult, error) {
	streamInfo := &CmdXinfoStreamResult{}
	cmd := client.B().XinfoStream().Key(stream).Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		if err.Error() == "no such key" {
			return nil, ErrNoSuchStream
		}
		return nil, err
	}

	streamMap, err := result.ToMap()
	if err != nil {
		return nil, fmt.Errorf("result toArray: %w", err)
	}

	redisMsg := streamMap["length"]
	length, err := redisMsg.ToInt64()
	if err != nil {
		return nil, fmt.Errorf("length: %w", err)
	}
	streamInfo.Length = length

	redisMsg = streamMap["radix-tree-keys"]
	radisTreeKeys, err := redisMsg.ToInt64()
	if err != nil {
		return nil, fmt.Errorf("radix-tree-keys: %w", err)
	}
	streamInfo.RadixTreeKeys = radisTreeKeys

	redisMsg = streamMap["radix-tree-nodes"]
	radisTreeNodes, err := redisMsg.ToInt64()
	if err != nil {
		return nil, fmt.Errorf("radix-tree-nodes: %w", err)
	}
	streamInfo.RadixTreeNodes = radisTreeNodes

	redisMsg = streamMap["last-generated-id"]
	lastGeneratedId, err := redisMsg.ToString()
	if err != nil {
		return nil, fmt.Errorf("last-generated-id: %w", err)
	}
	streamInfo.LastGeneratedId = lastGeneratedId

	redisMsg = streamMap["max-deleted-entry-id"]
	maxDeletedEntryId, err := redisMsg.ToString()
	if err != nil {
		return nil, fmt.Errorf("max-deleted-entry-id: %w", err)
	}
	streamInfo.MaxDeletedEntryId = maxDeletedEntryId

	redisMsg = streamMap["entries-added"]
	entriesAdded, err := redisMsg.ToInt64()
	if err != nil {
		return nil, fmt.Errorf("entries-added: %w", err)
	}
	streamInfo.EntriesAdded = entriesAdded

	redisMsg = streamMap["groups"]
	groups, err := redisMsg.ToInt64()
	if err != nil {
		return nil, fmt.Errorf("groups: %w", err)
	}
	streamInfo.Groups = groups

	redisMsg = streamMap["first-entry"]
	firstEntry, err := redisMsg.AsXRangeEntry()
	if err != nil {
		return nil, fmt.Errorf("first-entry: %w", err)
	}
	streamInfo.FirstEntry = firstEntry

	redisMsg = streamMap["last-entry"]
	lastEntry, err := redisMsg.AsXRangeEntry()
	if err != nil {
		return nil, fmt.Errorf("last-entry: %w", err)
	}
	streamInfo.LastEentry = lastEntry

	return streamInfo, nil
}

func CmdLPush(ctx context.Context, client rueidis.Client, key string, element ...string) (int64, error) {
	cmd := client.B().Lpush().Key(key).Element(element...).Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		return 0, err
	}
	n, err := result.ToInt64()
	if err != nil {
		return 0, err
	}
	return n, nil
}

func CmdRPush(ctx context.Context, client rueidis.Client, key string, element ...string) (int64, error) {
	cmd := client.B().Rpush().Key(key).Element(element...).Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		return 0, err
	}
	n, err := result.ToInt64()
	if err != nil {
		return 0, err
	}
	return n, nil
}

type Direction uint8

const (
	Left  Direction = 0
	Right Direction = 1
)

func CmdBLMove(
	ctx context.Context, client rueidis.Client,
	source string, destination string, direction1 Direction, direction2 Direction,
	timeout float64,
) (string, error) {
	cmd := blMoveCmdGen(client, source, destination, direction1, direction2, timeout)
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return "", nil
		}
		return "", err
	}
	strResult, err := result.ToString()
	if err != nil {
		return "", err
	}
	return strResult, nil
}

func blMoveCmdGen(client rueidis.Client,
	source string, destination string, direction1 Direction, direction2 Direction,
	timeout float64,
) rueidis.Completed {
	if direction1 == Left && direction2 == Left {
		return client.B().Blmove().Source(source).Destination(destination).Left().Left().Timeout(timeout).Build()
	} else if direction1 == Left && direction2 == Right {
		return client.B().Blmove().Source(source).Destination(destination).Left().Right().Timeout(timeout).Build()
	} else if direction1 == Right && direction2 == Left {
		return client.B().Blmove().Source(source).Destination(destination).Right().Left().Timeout(timeout).Build()
	} else if direction1 == Right && direction2 == Right {
		return client.B().Blmove().Source(source).Destination(destination).Right().Right().Timeout(timeout).Build()
	} else {
		panic("unreachable")
	}
}

func CmdLrem(ctx context.Context, client rueidis.Client, key string, count int64, element string) (int64, error) {
	cmd := client.B().Lrem().Key(key).Count(count).Element(element).Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		return 0, err
	}
	n, err := result.ToInt64()
	if err != nil {
		return 0, err
	}
	return n, nil
}

func CmdFlushAll(ctx context.Context, client rueidis.Client) error {
	cmd := client.B().Flushall().Sync().Build()
	err := client.Do(ctx, cmd).Error()
	if err != nil {
		return err
	}
	return nil
}

func CmdHGetAll(ctx context.Context, client rueidis.Client, key string) (map[string]string, error) {
	cmd := client.B().Hgetall().Key(key).Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		return nil, err
	}
	retVal := map[string]string{}
	resultMap, err := result.ToMap()
	if err != nil {
		return nil, err
	}
	for key, redisVal := range resultMap {
		val, err := redisVal.ToString()
		if err != nil {
			return nil, err
		}
		retVal[key] = val
	}
	return retVal, nil
}

func CmdLrange(ctx context.Context, client rueidis.Client, key string, start int64, stop int64) ([]string, error) {
	cmd := client.B().Lrange().Key(key).Start(start).Stop(stop).Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		return nil, err
	}
	retVal, err := result.AsStrSlice()
	if err != nil {
		return nil, err
	}
	return retVal, nil
}

func CmdHset(ctx context.Context, client rueidis.Client, key string, seq iter.Seq2[string, string]) (int64, error) {
	cmd := client.B().Hset().Key(key).FieldValue().FieldValueIter(seq).Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		return 0, err
	}
	n, err := result.ToInt64()
	if err != nil {
		return 0, err
	}
	return n, nil
}

func CmdZrange(ctx context.Context, client rueidis.Client, key string, start int, stopInclusive int) ([]string, error) {
	min := strconv.Itoa(start)
	max := strconv.Itoa(stopInclusive)
	cmd := client.B().Zrange().Key(key).Min(min).Max(max).Build()
	result := client.Do(ctx, cmd)
	err := result.Error()
	if err != nil {
		return nil, err
	}
	arrResult, err := result.AsStrSlice()
	if err != nil {
		return nil, err
	}
	return arrResult, nil
}

package redis

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func NewId() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func TestXgroupCreate(t *testing.T) {
	client, err := NewClientFromEnv(TestEnvs())
	require.Nil(t, err)
	defer client.Close()

	err = CmdFlushAll(context.Background(), client)
	require.Nil(t, err)

	id, err := NewId()
	require.Nil(t, err)

	stream := fmt.Sprintf("stream_%s", id)
	group := fmt.Sprintf("group_%s", id)

	streamExists, err := CmdExists(context.Background(), client, stream)
	require.Nil(t, err)
	require.False(t, streamExists)

	err = CmdXgroupCreate(context.Background(), client, stream, group, "$")
	require.Nil(t, err)

	streamExists, err = CmdExists(context.Background(), client, stream)
	require.Nil(t, err)
	require.True(t, streamExists)
}

func TestXgroupCreateConsumer(t *testing.T) {
	client, err := NewClientFromEnv(TestEnvs())
	require.Nil(t, err)
	defer client.Close()

	err = CmdFlushAll(context.Background(), client)
	require.Nil(t, err)

	id, err := NewId()
	require.Nil(t, err)

	stream := fmt.Sprintf("stream_%s", id)
	group := fmt.Sprintf("group_%s", id)
	consumer := fmt.Sprintf("consumer_%s", id)

	err = CmdXgroupCreate(context.Background(), client, stream, group, "$")
	require.Nil(t, err)

	err = CmdXgroupCreateConsumer(context.Background(), client, stream, group, consumer)
	require.Nil(t, err)

	err = CmdXgroupCreateConsumer(context.Background(), client, stream, group, consumer)
	require.Nil(t, err)
}

func TestXadd(t *testing.T) {
	client, err := NewClientFromEnv(TestEnvs())
	require.Nil(t, err)
	defer client.Close()

	err = CmdFlushAll(context.Background(), client)
	require.Nil(t, err)

	id, err := NewId()
	require.Nil(t, err)

	stream := fmt.Sprintf("stream_%s", id)
	group := fmt.Sprintf("group_%s", id)

	err = CmdXgroupCreate(context.Background(), client, stream, group, "$")
	require.Nil(t, err)

	data, err := CmdXadd(context.Background(), client, stream, map[string]string{"1": "1", "2": "2"})
	require.Nil(t, err)
	require.NotEqual(t, 0, len(data))
}

func TestXreadGroup(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		client, err := NewClientFromEnv(TestEnvs())
		require.Nil(t, err)
		defer client.Close()

		err = CmdFlushAll(context.Background(), client)
		require.Nil(t, err)

		id, err := NewId()
		require.Nil(t, err)

		stream := fmt.Sprintf("stream_%s", id)
		group := fmt.Sprintf("group_%s", id)
		consumer := fmt.Sprintf("name_%s", id)

		data := map[string]string{"1": "1", "2": "2"}
		_, err = CmdXadd(context.Background(), client, stream, data)
		require.Nil(t, err)

		err = CmdXgroupCreate(context.Background(), client, stream, group, "0")
		require.Nil(t, err)

		result, err := CmdXreadGroup(context.Background(), client, stream, group, consumer, ">", 1, 1)
		require.Nil(t, err)
		require.NotNil(t, result)
		require.Len(t, result, 1)
		require.Equal(t, data, result[0].FieldValues)
		require.NotEqual(t, 0, len(result[0].ID))
	})

	t.Run("empty_response", func(t *testing.T) {
		client, err := NewClientFromEnv(TestEnvs())
		require.Nil(t, err)
		defer client.Close()

		err = CmdFlushAll(context.Background(), client)
		require.Nil(t, err)

		id, err := NewId()
		require.Nil(t, err)

		stream := fmt.Sprintf("stream_%s", id)
		group := fmt.Sprintf("group_%s", id)
		consumer := fmt.Sprintf("name_%s", id)

		err = CmdXgroupCreate(context.Background(), client, stream, group, "0")
		require.Nil(t, err)

		tests := []struct {
			name    string
			blockMs int64
			msgId   string
		}{
			{name: "new_msg_block", msgId: ">", blockMs: 1},
			{name: "new_msg_non_block", msgId: ">", blockMs: 0},
			{name: "pending_msg_block", msgId: "0", blockMs: 1},
			{name: "pending_msg_non_block", msgId: "0", blockMs: 0},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				data, err := CmdXreadGroup(
					context.Background(), client, stream, group, consumer,
					test.msgId, 1, test.blockMs,
				)
				require.Nil(t, err)
				require.Len(t, data, 0)
			})
		}
	})
}

func TestXack(t *testing.T) {
	client, err := NewClientFromEnv(TestEnvs())
	require.Nil(t, err)
	defer client.Close()

	err = CmdFlushAll(context.Background(), client)
	require.Nil(t, err)

	id, err := NewId()
	require.Nil(t, err)

	stream := fmt.Sprintf("stream_%s", id)
	group := fmt.Sprintf("group_%s", id)
	consumer := fmt.Sprintf("consumer_%s", id)

	err = CmdXgroupCreate(context.Background(), client, stream, group, "0")
	require.Nil(t, err)

	_, err = CmdXadd(context.Background(), client, stream, map[string]string{"1": "1"})
	require.Nil(t, err)

	data, err := CmdXreadGroup(context.Background(), client, stream, group, consumer, ">", 1, 1)
	require.Nil(t, err)
	require.Len(t, data, 1)

	msgId := data[0].ID

	n, err := CmdXack(context.Background(), client, stream, group, msgId)
	require.Nil(t, err)
	require.Equal(t, int64(1), n)

	n, err = CmdXack(context.Background(), client, stream, group, msgId)
	require.Nil(t, err)
	require.Equal(t, int64(0), n)
}

func TestCmdTime(t *testing.T) {
	client, err := NewClientFromEnv(TestEnvs())
	require.Nil(t, err)
	defer client.Close()

	err = CmdFlushAll(context.Background(), client)
	require.Nil(t, err)

	_, err = CmdTime(context.Background(), client)
	require.Nil(t, err)
}

func TestXinfoGroups(t *testing.T) {
	t.Run("stream_not_exists", func(t *testing.T) {
		client, err := NewClientFromEnv(TestEnvs())
		require.Nil(t, err)
		defer client.Close()

		err = CmdFlushAll(context.Background(), client)
		require.Nil(t, err)

		id, err := NewId()
		require.Nil(t, err)

		stream := fmt.Sprintf("stream_%s", id)

		_, err = CmdXinfoGroups(context.Background(), client, stream)
		require.ErrorIs(t, err, ErrNoSuchStream)
	})
}

func TestXinfoStream(t *testing.T) {
	client, err := NewClientFromEnv(TestEnvs())
	require.Nil(t, err)
	defer client.Close()

	err = CmdFlushAll(context.Background(), client)
	require.Nil(t, err)

	id, err := NewId()
	require.Nil(t, err)

	stream := fmt.Sprintf("stream_%s", id)
	group := fmt.Sprintf("group_%s", id)

	data1 := map[string]string{"data": "1"}
	data2 := map[string]string{"data": "2"}

	_, err = CmdXadd(context.Background(), client, stream, data1)
	require.Nil(t, err)
	_, err = CmdXadd(context.Background(), client, stream, data2)
	require.Nil(t, err)

	err = CmdXgroupCreate(context.Background(), client, stream, group, "0")
	require.Nil(t, err)

	res, err := CmdXinfoStream(context.Background(), client, stream)
	require.Nil(t, err)
	require.NotNil(t, res)

	require.Equal(t, int64(2), res.Length)
	require.Equal(t, int64(1), res.Groups)
	require.NotEqual(t, "", res.LastGeneratedId)
	require.Equal(t, "0-0", res.MaxDeletedEntryId)
	require.Equal(t, int64(2), res.EntriesAdded)
	require.Equal(t, data1, res.FirstEntry.FieldValues)
	require.Equal(t, data2, res.LastEentry.FieldValues)
}

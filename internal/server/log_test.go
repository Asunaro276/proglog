package server

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	t.Run("append and read a record", func(t *testing.T) {
		log := NewLog()
		record := Record{
			Value: []byte("hello world"),
		}
		offset, err := log.Append(record)
		require.NoError(t, err)
		require.Equal(t, uint64(0), offset)

		read, err := log.Read(offset)
		require.NoError(t, err)
		require.Equal(t, record.Value, read.Value)
		require.Equal(t, offset, read.Offset)
	})

	t.Run("read out of range offset", func(t *testing.T) {
		log := NewLog()
		_, err := log.Read(0)
		require.Error(t, err)
		require.Equal(t, ErrOffsetNotFound, err)
	})

	t.Run("append multiple records", func(t *testing.T) {
		log := NewLog()
		records := []Record{
			{Value: []byte("first")},
			{Value: []byte("second")},
			{Value: []byte("third")},
		}

		for i, record := range records {
			offset, err := log.Append(record)
			require.NoError(t, err)
			require.Equal(t, uint64(i), offset)
		}

		// Read all records and verify
		for i, expected := range records {
			read, err := log.Read(uint64(i))
			require.NoError(t, err)
			require.Equal(t, expected.Value, read.Value)
			require.Equal(t, uint64(i), read.Offset)
		}
	})

	t.Run("concurrent operations", func(t *testing.T) {
		log := NewLog()
		numRecords := 100
		done := make(chan struct{})

		// Concurrent appends
		go func() {
			for i := 0; i < numRecords; i++ {
				_, err := log.Append(Record{
					Value: []byte("concurrent"),
				})
				require.NoError(t, err)
			}
			close(done)
		}()

		<-done

		// Verify all records were appended
		require.Equal(t, numRecords, len(log.records))

		// Verify we can read all records
		for i := 0; i < numRecords; i++ {
			_, err := log.Read(uint64(i))
			require.NoError(t, err)
		}
	})
}
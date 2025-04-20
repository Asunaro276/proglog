package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleProduce(t *testing.T) {
	srv := newHTTPServer()
	srv.Log = &Log{}

	testCases := []struct {
		name          string
		record        Record
		expectedCode  int
		expectedError string
	}{
		{
			name: "正常なProduce処理",
			record: Record{
				Value: []byte("hello world"),
			},
			expectedCode: http.StatusOK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reqBody, err := json.Marshal(ProduceRequest{
				Record: tc.record,
			})
			require.NoError(t, err)

			req, err := http.NewRequest(
				http.MethodPost,
				"/",
				bytes.NewBuffer(reqBody),
			)
			require.NoError(t, err)

			rr := httptest.NewRecorder()
			srv.handleProduce(rr, req)

			require.Equal(t, tc.expectedCode, rr.Code)

			if tc.expectedError == "" {
				var resp ProduceResponse
				err = json.NewDecoder(rr.Body).Decode(&resp)
				require.NoError(t, err)
				require.Equal(t, uint64(0), resp.Offset)
			} else {
				require.Contains(t, rr.Body.String(), tc.expectedError)
			}
		})
	}
}

func TestHandleConsume(t *testing.T) {
	srv := newHTTPServer()
	srv.Log = &Log{}

	// 事前にレコードを追加
	record := Record{Value: []byte("hello world")}
	_, err := srv.Log.Append(record)
	require.NoError(t, err)

	testCases := []struct {
		name          string
		offset        uint64
		expectedCode  int
		expectedError string
	}{
		{
			name:         "正常なConsume処理",
			offset:       0,
			expectedCode: http.StatusOK,
		},
		{
			name:          "存在しないオフセット",
			offset:        1,
			expectedCode:  http.StatusNotFound,
			expectedError: ErrOffsetNotFound.Error(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reqBody, err := json.Marshal(ConsumeRequest{
				Offset: tc.offset,
			})
			require.NoError(t, err)

			req, err := http.NewRequest(
				http.MethodGet,
				"/",
				bytes.NewBuffer(reqBody),
			)
			require.NoError(t, err)

			rr := httptest.NewRecorder()
			srv.handleConsume(rr, req)

			require.Equal(t, tc.expectedCode, rr.Code)

			if tc.expectedError == "" {
				var resp ConsumeResponse
				err = json.NewDecoder(rr.Body).Decode(&resp)
				require.NoError(t, err)
				require.Equal(t, record.Value, resp.Record.Value)
				require.Equal(t, uint64(0), resp.Record.Offset)
			} else {
				require.Contains(t, rr.Body.String(), tc.expectedError)
			}
		})
	}
}

func TestHTTPServer(t *testing.T) {
	server := NewHTTPServer(":8080")
	require.Equal(t, ":8080", server.Addr)
	require.NotNil(t, server.Handler)
}

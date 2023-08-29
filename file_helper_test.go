package cloudstorage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestETAG(t *testing.T) {
	require.Equal(t, "hello", CleanETag("hello"))
	require.Equal(t, "hello", CleanETag(`"hello"`))
	require.Equal(t, "hello", CleanETag(`\"hello\"`))
	require.Equal(t, "hello", CleanETag("\"hello\""))
}
func TestContentType(t *testing.T) {
	require.Equal(t, "text/csv; charset=utf-8", ContentType("data.csv"))
	require.Equal(t, "application/json", ContentType("data.json"))
	require.Equal(t, "application/octet-stream", ContentType("data.unknown"))
}

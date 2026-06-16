package chhttp

import (
	"net/url"
	"testing"
)

func TestEndpointIncludesQueryIDAndSettings(t *testing.T) {
	client := Client{URL: "http://clickhouse:8123/?database=default"}
	endpoint, err := client.endpoint(QueryOptions{
		QueryID: "partforge-query",
		Settings: QuerySettings{
			"max_threads":        "8",
			"max_insert_threads": "8",
			"max_memory_usage":   "12345",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	if query.Get("database") != "default" {
		t.Fatalf("database = %q", query.Get("database"))
	}
	if query.Get("query_id") != "partforge-query" {
		t.Fatalf("query_id = %q", query.Get("query_id"))
	}
	if query.Get("max_threads") != "8" {
		t.Fatalf("max_threads = %q", query.Get("max_threads"))
	}
	if query.Get("max_insert_threads") != "8" {
		t.Fatalf("max_insert_threads = %q", query.Get("max_insert_threads"))
	}
	if query.Get("max_memory_usage") != "12345" {
		t.Fatalf("max_memory_usage = %q", query.Get("max_memory_usage"))
	}
}

func TestEndpointRejectsEmptySettingName(t *testing.T) {
	client := Client{URL: "http://clickhouse:8123/"}
	if _, err := client.endpoint(QueryOptions{Settings: QuerySettings{"": "1"}}); err == nil {
		t.Fatal("expected empty setting name error")
	}
}

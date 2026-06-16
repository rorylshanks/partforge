package chhttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type Client struct {
	URL      string
	User     string
	Password string
}

type QueryOptions struct {
	QueryID  string
	Settings QuerySettings
}

type QuerySettings map[string]string

type QueryError struct {
	StatusCode int
	Body       string
}

func (e *QueryError) Error() string {
	return fmt.Sprintf("clickhouse query failed with status %d: %s", e.StatusCode, strings.TrimSpace(e.Body))
}

func (c Client) Exec(ctx context.Context, query string) error {
	_, err := c.QueryString(ctx, query)
	return err
}

func (c Client) ExecWithOptions(ctx context.Context, query string, opts QueryOptions) error {
	_, err := c.QueryStringWithOptions(ctx, query, opts)
	return err
}

func (c Client) QueryString(ctx context.Context, query string) (string, error) {
	return c.QueryStringWithOptions(ctx, query, QueryOptions{})
}

func (c Client) QueryStringWithOptions(ctx context.Context, query string, opts QueryOptions) (string, error) {
	if strings.TrimSpace(c.URL) == "" {
		return "", fmt.Errorf("clickhouse URL is empty")
	}
	endpoint, err := c.endpoint(opts)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBufferString(query))
	if err != nil {
		return "", err
	}
	if c.User != "" {
		req.SetBasicAuth(c.User, c.Password)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", &QueryError{StatusCode: resp.StatusCode, Body: string(body)}
	}
	return string(body), nil
}

func (c Client) endpoint(opts QueryOptions) (string, error) {
	u, err := url.Parse(c.URL)
	if err != nil {
		return "", err
	}
	if opts.QueryID != "" || len(opts.Settings) > 0 {
		q := u.Query()
		if opts.QueryID != "" {
			q.Set("query_id", opts.QueryID)
		}
		for key, value := range opts.Settings {
			if strings.TrimSpace(key) == "" {
				return "", fmt.Errorf("clickhouse setting name is empty")
			}
			q.Set(key, value)
		}
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}

func (c Client) Ping(ctx context.Context) error {
	_, err := c.QueryString(ctx, "SELECT 1")
	return err
}

func TableSQL(database, table string) string {
	return Ident(database) + "." + Ident(table)
}

func Ident(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func StringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func URLWithDatabase(rawURL, database string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("database", database)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func FormatTSVStrings(rows string, columns int) ([][]string, error) {
	var out [][]string
	for _, line := range strings.Split(strings.TrimSpace(rows), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != columns {
			return nil, fmt.Errorf("expected %d TSV columns, got %d in %q", columns, len(fields), line)
		}
		out = append(out, fields)
	}
	return out, nil
}

func ParseUInt(s string) (uint64, error) {
	v, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse uint %q: %w", s, err)
	}
	return v, nil
}

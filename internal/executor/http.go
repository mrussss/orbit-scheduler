package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

type HTTPConfig struct {
	AllowedHosts                                     []string
	RequestTimeout, DialTimeout, TLSHandshakeTimeout time.Duration
	MaxRequestBytes, MaxResponseBytes                int64
	MaxRedirects                                     int
}
type HTTP struct {
	cfg      HTTPConfig
	allowed  map[string]struct{}
	client   *http.Client
	resolver *net.Resolver
	dialer   *net.Dialer
}
type httpPayload struct {
	Method         string            `json:"method"`
	URL            string            `json:"url"`
	Headers        map[string]string `json:"headers"`
	Body           json.RawMessage   `json:"body"`
	IdempotencyKey string            `json:"idempotency_key"`
	SuccessMin     int               `json:"success_min"`
	SuccessMax     int               `json:"success_max"`
}

func NewHTTP(cfg HTTPConfig) (*HTTP, error) {
	if len(cfg.AllowedHosts) == 0 {
		return nil, errors.New("http executor requires an allowlist")
	}
	if cfg.RequestTimeout <= 0 || cfg.DialTimeout <= 0 || cfg.MaxRequestBytes <= 0 || cfg.MaxResponseBytes <= 0 || cfg.MaxRedirects < 0 {
		return nil, errors.New("invalid http executor configuration")
	}
	allowed := map[string]struct{}{}
	for _, host := range cfg.AllowedHosts {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" {
			return nil, errors.New("empty allowed host")
		}
		allowed[host] = struct{}{}
	}
	executor := &HTTP{cfg: cfg, allowed: allowed, resolver: net.DefaultResolver, dialer: &net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: 30 * time.Second}}
	transport := &http.Transport{Proxy: nil, DialContext: executor.safeDialContext, ForceAttemptHTTP2: true, MaxIdleConns: 32, MaxIdleConnsPerHost: 8, IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: cfg.TLSHandshakeTimeout, ResponseHeaderTimeout: cfg.RequestTimeout, ExpectContinueTimeout: time.Second}
	executor.client = &http.Client{Transport: transport, Timeout: cfg.RequestTimeout, CheckRedirect: executor.checkRedirect}
	return executor, nil
}
func (e *HTTP) Execute(ctx context.Context, task scheduler.Assignment) Result {
	started := time.Now().UTC()
	var payload httpPayload
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "invalid http payload")
	}
	payload.Method = strings.ToUpper(payload.Method)
	if payload.Method != "GET" && payload.Method != "POST" {
		return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "method must be GET or POST")
	}
	target, err := url.Parse(payload.URL)
	if err != nil {
		return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "invalid URL")
	}
	if err := e.validateURL(ctx, target); err != nil {
		return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, err.Error())
	}
	if len(payload.Body) > int(e.cfg.MaxRequestBytes) {
		return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "request body too large")
	}
	var body io.Reader
	if payload.Method == "POST" {
		body = bytes.NewReader(payload.Body)
	}
	request, err := http.NewRequestWithContext(ctx, payload.Method, target.String(), body)
	if err != nil {
		return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "build request failed")
	}
	for name, value := range payload.Headers {
		if forbiddenHeader(name) {
			return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "sensitive header is forbidden")
		}
		request.Header.Set(name, value)
	}
	idempotencyKey := payload.IdempotencyKey
	if idempotencyKey == "" {
		idempotencyKey = task.TaskID.String() + ":" + strconv.Itoa(task.AttemptNo)
	}
	request.Header.Set("Idempotency-Key", idempotencyKey)
	response, err := e.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return canceledResult(started, ctx)
		}
		return failure(started, domain.OutcomeRetryableFailure, domain.ErrorTransport, "http request failed: "+err.Error())
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, e.cfg.MaxResponseBytes+1)
	responseBody, err := io.ReadAll(limited)
	if err != nil {
		return failure(started, domain.OutcomeRetryableFailure, domain.ErrorTransport, "read response failed")
	}
	if int64(len(responseBody)) > e.cfg.MaxResponseBytes {
		return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "response body too large")
	}
	minimum, maximum := payload.SuccessMin, payload.SuccessMax
	if minimum == 0 {
		minimum = 200
	}
	if maximum == 0 {
		maximum = 299
	}
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
		return failure(started, domain.OutcomeRetryableFailure, domain.ErrorTransport, fmt.Sprintf("upstream status %d", response.StatusCode))
	}
	if response.StatusCode < minimum || response.StatusCode > maximum {
		return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, fmt.Sprintf("unexpected upstream status %d", response.StatusCode))
	}
	result, _ := json.Marshal(map[string]any{"status_code": response.StatusCode, "body": string(responseBody), "content_type": response.Header.Get("Content-Type")})
	return resultSuccess(started, result)
}
func (e *HTTP) validateURL(ctx context.Context, target *url.URL) error {
	if target.Scheme != "http" && target.Scheme != "https" {
		return errors.New("URL scheme must be http or https")
	}
	host := strings.ToLower(strings.TrimSuffix(target.Hostname(), "."))
	if host == "" || !e.hostAllowed(host) {
		return errors.New("URL host is not allowed")
	}
	addresses, err := e.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return fmt.Errorf("resolve URL host: %w", err)
	}
	if len(addresses) == 0 {
		return errors.New("URL host has no addresses")
	}
	for _, address := range addresses {
		if !safePublicIP(address.IP) {
			return errors.New("URL resolves to a non-public address")
		}
	}
	return nil
}
func (e *HTTP) hostAllowed(host string) bool {
	if _, ok := e.allowed[host]; ok {
		return true
	}
	for allowed := range e.allowed {
		if strings.HasPrefix(allowed, "*.") && strings.HasSuffix(host, allowed[1:]) && host != allowed[2:] {
			return true
		}
	}
	return false
}
func (e *HTTP) safeDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	addresses, err := e.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	for _, address := range addresses {
		if safePublicIP(address.IP) {
			return e.dialer.DialContext(ctx, network, net.JoinHostPort(address.IP.String(), port))
		}
	}
	return nil, errors.New("dial target has no public address")
}
func (e *HTTP) checkRedirect(request *http.Request, via []*http.Request) error {
	if len(via) > e.cfg.MaxRedirects {
		return errors.New("too many redirects")
	}
	if err := e.validateURL(request.Context(), request.URL); err != nil {
		return err
	}
	for name := range request.Header {
		if forbiddenHeader(name) {
			request.Header.Del(name)
		}
	}
	return nil
}
func forbiddenHeader(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "authorization", "proxy-authorization", "cookie", "set-cookie", "host", "forwarded",
		"x-forwarded-for", "x-forwarded-host", "x-forwarded-proto", "x-real-ip",
		"x-original-url", "x-rewrite-url":
		return true
	default:
		return false
	}
}
func safePublicIP(ip net.IP) bool {
	if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	reserved := []*net.IPNet{
		mustCIDR("0.0.0.0/8"),
		mustCIDR("100.64.0.0/10"),
		mustCIDR("192.0.0.0/24"),
		mustCIDR("192.0.2.0/24"),
		mustCIDR("192.88.99.0/24"),
		mustCIDR("198.18.0.0/15"),
		mustCIDR("198.51.100.0/24"),
		mustCIDR("203.0.113.0/24"),
		mustCIDR("240.0.0.0/4"),
		mustCIDR("100::/64"),
		mustCIDR("2001:2::/48"),
		mustCIDR("2001:db8::/32"),
	}
	for _, network := range reserved {
		if network.Contains(ip) {
			return false
		}
	}
	return ip.IsGlobalUnicast()
}
func mustCIDR(raw string) *net.IPNet {
	_, network, err := net.ParseCIDR(raw)
	if err != nil {
		panic(err)
	}
	return network
}

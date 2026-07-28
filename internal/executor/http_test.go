package executor

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestSafePublicIP(t *testing.T) {
	for _, raw := range []string{"0.1.2.3", "127.0.0.1", "10.0.0.1", "100.64.0.1", "169.254.169.254", "192.0.0.1", "192.0.2.1", "198.18.0.1", "::1", "100::1", "2001:2::1", "2001:db8::1"} {
		if safePublicIP(net.ParseIP(raw)) {
			t.Fatalf("unsafe address accepted: %s", raw)
		}
	}
	if !safePublicIP(net.ParseIP("8.8.8.8")) {
		t.Fatal("public address rejected")
	}
}

func TestHTTPExecutorRejectsPrivateRedirect(t *testing.T) {
	executor, err := NewHTTP(HTTPConfig{AllowedHosts: []string{"localhost"}, RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, MaxRequestBytes: 1024, MaxResponseBytes: 1024, MaxRedirects: 1})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://localhost/metadata", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.checkRedirect(request, []*http.Request{{}}); err == nil {
		t.Fatal("redirect to private resolution accepted")
	}
}
func TestHTTPExecutorRejectsPrivateResolutionAndSensitiveHeaders(t *testing.T) {
	executor, err := NewHTTP(HTTPConfig{AllowedHosts: []string{"localhost"}, RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, MaxRequestBytes: 1024, MaxResponseBytes: 1024, MaxRedirects: 1})
	if err != nil {
		t.Fatal(err)
	}
	target, _ := url.Parse("http://localhost/")
	if err := executor.validateURL(context.Background(), target); err == nil {
		t.Fatal("localhost accepted")
	}
	for _, header := range []string{"Authorization", "Cookie", "Host", "Forwarded", "X-Forwarded-For", "X-Original-URL"} {
		if !forbiddenHeader(header) {
			t.Fatalf("sensitive header accepted: %s", header)
		}
	}
}

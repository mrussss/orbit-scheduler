package middleware

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/business"
	"github.com/mrussss/orbit-scheduler/internal/httpx"
	"github.com/prometheus/client_golang/prometheus"
)

const PrincipalKey = "principal"

type Authenticator interface {
	Authenticate(context.Context, string) (business.Principal, error)
	IsAdmin(string) bool
}

func RequestContext(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := strings.TrimSpace(c.GetHeader("X-Request-ID"))
		if requestID == "" || len(requestID) > 128 {
			requestID = "req-" + uuid.NewString()
		}
		traceID := strings.TrimSpace(c.GetHeader("X-Trace-ID"))
		if traceID == "" || len(traceID) > 128 {
			traceID = "trace-" + uuid.NewString()
		}
		c.Set("request_id", requestID)
		c.Set("trace_id", traceID)
		c.Header("X-Request-ID", requestID)
		started := time.Now()
		c.Next()
		logger.Info("http request", "request_id", requestID, "trace_id", traceID, "method", c.Request.Method, "path", c.FullPath(), "status", c.Writer.Status(), "duration_ms", time.Since(started).Milliseconds())
	}
}
func Recover(logger *slog.Logger) gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered any) {
		logger.Error("http panic recovered", "request_id", c.GetString("request_id"), "panic", recovered)
		httpx.WriteError(c, http.StatusInternalServerError, "INTERNAL", "internal server error", nil)
	})
}
func BodyLimit(max int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, max)
		}
		c.Next()
	}
}
func Timeout(duration time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), duration)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		if ctx.Err() == context.DeadlineExceeded && !c.Writer.Written() {
			httpx.WriteError(c, http.StatusGatewayTimeout, "REQUEST_TIMEOUT", "request deadline exceeded", nil)
		}
	}
}
func CORS(allowedOrigins []string) gin.HandlerFunc {
	allowed := map[string]struct{}{}
	for _, origin := range allowedOrigins {
		allowed[origin] = struct{}{}
	}
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if _, ok := allowed[origin]; ok {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
			c.Header("Access-Control-Allow-Headers", "Authorization,Content-Type,Idempotency-Key,X-Request-ID,X-Trace-ID")
			c.Header("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS")
		}
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
func TenantAuth(authenticator Authenticator) gin.HandlerFunc {
	return func(c *gin.Context) {
		plain, ok := bearer(c.GetHeader("Authorization"))
		if !ok {
			httpx.WriteError(c, http.StatusUnauthorized, "UNAUTHORIZED", "valid bearer token required", nil)
			return
		}
		principal, err := authenticator.Authenticate(c.Request.Context(), plain)
		if err != nil {
			httpx.WriteError(c, http.StatusUnauthorized, "UNAUTHORIZED", "valid bearer token required", nil)
			return
		}
		c.Set(PrincipalKey, principal)
		c.Next()
	}
}
func AdminAuth(authenticator Authenticator) gin.HandlerFunc {
	return func(c *gin.Context) {
		plain, ok := bearer(c.GetHeader("Authorization"))
		if !ok || !authenticator.IsAdmin(plain) {
			httpx.WriteError(c, http.StatusUnauthorized, "ADMIN_UNAUTHORIZED", "valid administrator token required", nil)
			return
		}
		c.Next()
	}
}
func AdminOrProject(authenticator Authenticator) gin.HandlerFunc {
	return func(c *gin.Context) {
		plain, ok := bearer(c.GetHeader("Authorization"))
		if !ok {
			httpx.WriteError(c, 401, "UNAUTHORIZED", "valid administrator or project token required", nil)
			return
		}
		if authenticator.IsAdmin(plain) {
			c.Next()
			return
		}
		principal, err := authenticator.Authenticate(c.Request.Context(), plain)
		if err != nil || !principal.Has("project:admin") {
			httpx.WriteError(c, 403, "SCOPE_REQUIRED", "project administrator scope required", nil)
			return
		}
		projectID, err := uuid.Parse(c.Param("project_id"))
		if err != nil || projectID != principal.ProjectID {
			httpx.WriteError(c, 404, "NOT_FOUND", "resource does not exist", nil)
			return
		}
		c.Set(PrincipalKey, principal)
		c.Next()
	}
}
func RequireScope(scope string) gin.HandlerFunc {
	return func(c *gin.Context) {
		value, ok := c.Get(PrincipalKey)
		if !ok {
			httpx.WriteError(c, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required", nil)
			return
		}
		principal, ok := value.(business.Principal)
		if !ok || !principal.Has(scope) {
			httpx.WriteError(c, http.StatusForbidden, "SCOPE_REQUIRED", "required scope is missing", map[string]any{"scope": scope})
			return
		}
		c.Next()
	}
}
func Principal(c *gin.Context) business.Principal {
	value, _ := c.Get(PrincipalKey)
	principal, _ := value.(business.Principal)
	return principal
}
func bearer(header string) (string, bool) {
	parts := strings.Fields(header)
	returnValue := ""
	if len(parts) == 2 && subtle.ConstantTimeCompare([]byte(strings.ToLower(parts[0])), []byte("bearer")) == 1 {
		returnValue = parts[1]
	}
	return returnValue, returnValue != ""
}

type HTTPMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func NewHTTPMetrics(registerer prometheus.Registerer) *HTTPMetrics {
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orbit_http_requests_total", Help: "HTTP requests by route and status."}, []string{"method", "route", "status"})
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "orbit_http_request_duration_seconds", Help: "HTTP request latency.", Buckets: prometheus.DefBuckets}, []string{"method", "route"})
	registerOrReuse(registerer, requests)
	registerOrReuse(registerer, duration)
	return &HTTPMetrics{requests, duration}
}
func (m *HTTPMetrics) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		status := http.StatusText(c.Writer.Status())
		m.requests.WithLabelValues(c.Request.Method, route, status).Inc()
		m.duration.WithLabelValues(c.Request.Method, route).Observe(time.Since(started).Seconds())
	}
}
func registerOrReuse(registerer prometheus.Registerer, collector prometheus.Collector) {
	if registerer == nil {
		return
	}
	_ = registerer.Register(collector)
}

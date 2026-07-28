package middleware

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/business"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeAuth struct{ principal business.Principal }

func (f fakeAuth) Authenticate(context.Context, string) (business.Principal, error) {
	return f.principal, nil
}
func (fakeAuth) IsAdmin(string) bool { return false }
func TestTenantScope(t *testing.T) {
	gin.SetMode(gin.TestMode)
	projectID := uuid.New()
	auth := fakeAuth{business.Principal{ProjectID: projectID, Scopes: map[string]struct{}{"task:read": {}}}}
	router := gin.New()
	router.GET("/tasks", TenantAuth(auth), RequireScope("task:write"), func(c *gin.Context) { c.Status(204) })
	request := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	request.Header.Set("Authorization", "Bearer orb_abcdefghijklmnopqrstuvwxyz0123456789")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

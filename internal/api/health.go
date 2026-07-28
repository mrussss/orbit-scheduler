package api

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type Pinger interface{ Ping(context.Context) error }

func HealthRouter(pinger Pinger) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.GET("/health/live", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "live"}) })
	router.GET("/health/ready", func(c *gin.Context) {
		if pinger == nil {
			c.JSON(http.StatusOK, gin.H{"status": "ready"})
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
		defer cancel()
		if err := pinger.Ping(ctx); err != nil {
			WriteError(c, http.StatusServiceUnavailable, "NOT_READY", "database is unavailable", nil)
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})
	return router
}

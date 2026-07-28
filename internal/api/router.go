package api

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mrussss/orbit-scheduler/internal/business"
	"github.com/mrussss/orbit-scheduler/internal/middleware"
	"github.com/prometheus/client_golang/prometheus"
)

type RouterConfig struct {
	MaxBodyBytes   int64
	RequestTimeout time.Duration
	AllowedOrigins []string
	CursorSecret   string
}

func NewRouter(logger *slog.Logger, service *business.Service, pinger Pinger, cfg RouterConfig) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	metrics := middleware.NewHTTPMetrics(prometheus.DefaultRegisterer)
	router.Use(middleware.RequestContext(logger), middleware.Recover(logger), middleware.CORS(cfg.AllowedOrigins), middleware.BodyLimit(cfg.MaxBodyBytes), middleware.Timeout(cfg.RequestTimeout), metrics.Middleware())
	registerHealth(router, pinger)
	handlers := newHandlers(service, cfg.CursorSecret)
	projects := router.Group("/api/v1/projects")
	projects.POST("", middleware.AdminAuth(service), handlers.createProject)
	projects.GET("", middleware.AdminAuth(service), handlers.listProjects)
	projectAdmin := projects.Group("/:project_id", middleware.AdminOrProject(service))
	projectAdmin.GET("", handlers.getProject)
	projectAdmin.PATCH("", handlers.updateProject)
	projectAdmin.POST("/tokens", handlers.createToken)
	projectAdmin.GET("/tokens", handlers.listTokens)
	projectAdmin.DELETE("/tokens/:token_id", handlers.disableToken)
	tenant := router.Group("/api/v1", middleware.TenantAuth(service))
	tenant.POST("/tasks", middleware.RequireScope("task:write"), handlers.createTask)
	tenant.GET("/tasks", middleware.RequireScope("task:read"), handlers.listTasks)
	tenant.GET("/tasks/:task_id", middleware.RequireScope("task:read"), handlers.getTask)
	tenant.POST("/tasks/:task_id/cancel", middleware.RequireScope("task:write"), handlers.cancelTask)
	tenant.GET("/tasks/:task_id/attempts", middleware.RequireScope("task:read"), handlers.listAttempts)
	tenant.GET("/tasks/:task_id/result", middleware.RequireScope("task:read"), handlers.getResult)
	tenant.POST("/jobs", middleware.RequireScope("job:write"), handlers.createJob)
	tenant.GET("/jobs", middleware.RequireScope("job:read"), handlers.listJobs)
	tenant.GET("/jobs/:job_id", middleware.RequireScope("job:read"), handlers.getJob)
	tenant.POST("/jobs/:job_id/cancel", middleware.RequireScope("job:write"), handlers.cancelJob)
	tenant.GET("/jobs/:job_id/tasks", middleware.RequireScope("job:read"), handlers.listJobTasks)
	return router
}

func registerHealth(router *gin.Engine, pinger Pinger) {
	router.GET("/health/live", func(c *gin.Context) { c.JSON(200, gin.H{"status": "live"}) })
	router.GET("/health/ready", func(c *gin.Context) {
		if pinger != nil {
			if err := pinger.Ping(c.Request.Context()); err != nil {
				WriteError(c, 503, "NOT_READY", "database is unavailable", nil)
				return
			}
		}
		c.JSON(200, gin.H{"status": "ready"})
	})
}

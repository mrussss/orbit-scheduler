package httpx

import "github.com/gin-gonic/gin"

type ErrorResponse struct {
	RequestID string         `json:"request_id"`
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Details   map[string]any `json:"details"`
}

func WriteError(c *gin.Context, status int, code, message string, details map[string]any) {
	if details == nil {
		details = map[string]any{}
	}
	c.AbortWithStatusJSON(status, ErrorResponse{RequestID: c.GetString("request_id"), Code: code, Message: message, Details: details})
}

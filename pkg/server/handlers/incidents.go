package handlers

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/porter-dev/porter-agent/pkg/models"
	"github.com/porter-dev/porter-agent/pkg/utils"
)

func GetAllIncidents(c *gin.Context) {
	incidentIDs, err := redisClient.GetAllIncidents(c.Copy())
	if err != nil {
		httpLogger.Error(err, "error getting list of all incidents")

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal server error",
		})
		return
	}

	var incidents []*models.Incident

	for _, id := range incidentIDs {
		incidentObj, err := utils.NewIncidentFromString(id)
		if err != nil {
			httpLogger.Error(err, "error getting incident object", "incidentID", id)

			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "internal server error",
			})
			return
		}

		incident := &models.Incident{
			ID:          id,
			ReleaseName: incidentObj.GetReleaseName(),
		}

		resolved, err := redisClient.IsIncidentResolved(c.Copy(), id)
		if err != nil {
			httpLogger.Error(err, "error checking if incident is resolved", "incidentID", id)

			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "internal server error",
			})
			return
		}

		if resolved {
			incident.LatestState = "RESOLVED"
		} else {
			incident.LatestState = "ONGOING"
		}

		incident.LatestReason, incident.LatestMessage, err = redisClient.GetLatestReasonAndMessage(c.Copy(), id)
		if err != nil {
			httpLogger.Error(err, "error fetching latest reason and messaged", "incidentID", id)

			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "internal server error",
			})
			return
		}

		incidents = append(incidents, incident)
	}

	c.JSON(http.StatusOK, gin.H{
		"incidents": incidents,
	})
}

func GetIncidentsByReleaseNamespace(c *gin.Context) {
	releaseName := c.Param("releaseName")
	namespace := c.Param("namespace")

	incidentIDs, err := redisClient.GetIncidentsByReleaseNamespace(c.Copy(), releaseName, namespace)
	if err != nil {
		httpLogger.Error(err, "error getting incidents for release", "releaseName", releaseName)

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal server error",
		})
		return
	}

	var incidents []*models.Incident

	for _, id := range incidentIDs {
		incidentObj, err := utils.NewIncidentFromString(id)
		if err != nil {
			httpLogger.Error(err, "error getting incident object from ID:", id)

			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "internal server error",
			})
			return
		}

		incident := &models.Incident{
			ID:          id,
			ReleaseName: incidentObj.GetReleaseName(),
		}

		resolved, err := redisClient.IsIncidentResolved(c.Copy(), id)
		if err != nil {
			httpLogger.Error(err, "error checking if incident is resolved", "incidentID", id)

			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "internal server error",
			})
			return
		}

		if resolved {
			incident.LatestState = "RESOLVED"
		} else {
			incident.LatestState = "ONGOING"
		}

		incident.LatestReason, incident.LatestMessage, err = redisClient.GetLatestReasonAndMessage(c.Copy(), id)
		if err != nil {
			httpLogger.Error(err, "error fetching latest reason and messaged", "incidentID", id)

			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "internal server error",
			})
			return
		}

		incidents = append(incidents, incident)
	}

	c.JSON(http.StatusOK, gin.H{
		"incidents": incidents,
	})
}

func GetIncidentEventsByID(c *gin.Context) {
	incidentID := c.Param("incidentID")

	exists, err := redisClient.IncidentExists(c.Copy(), incidentID)
	if err != nil {
		httpLogger.Error(err, "error checking for existence of incident", "incidentID", incidentID)

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal server error",
		})
		return
	}

	if !exists {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "invalid incident ID",
		})
		return
	}

	events, err := redisClient.GetIncidentEventsByID(c.Copy(), incidentID)
	if err != nil {
		httpLogger.Error(err, "error getting events for incident", "incidentID", incidentID)

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal server error",
		})
		return
	}

	resolved, err := redisClient.IsIncidentResolved(c.Copy(), incidentID)
	if err != nil {
		httpLogger.Error(err, "error checking if incident is resolved", "incidentID", incidentID)

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal server error",
		})
		return
	}

	latestState := "ONGOING"

	if resolved {
		latestState = "RESOLVED"
	}

	latestReason, latestMessage, err := redisClient.GetLatestReasonAndMessage(c.Copy(), incidentID)
	if err != nil {
		httpLogger.Error(err, "error fetching latest reason and messaged", "incidentID", incidentID)

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal server error",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"incident_id":    incidentID,
		"release_name":   strings.Split(incidentID, ":")[1],
		"latest_state":   latestState,
		"latest_reason":  latestReason,
		"latest_message": latestMessage,
		"events":         events,
	})
}

func GetLogs(c *gin.Context) {
	logID := c.Param("logID")

	logs, err := redisClient.GetLogs(c.Copy(), logID)
	if err != nil {
		if strings.Contains(err.Error(), "no such logs") {
			httpLogger.Error(err, "no such logs", "logID", logID)

			c.JSON(http.StatusNotFound, gin.H{
				"error": "no such logs",
			})
			return
		}

		httpLogger.Error(err, "error getting logs", "logID", logID)

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal server error",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"contents": logs,
	})
}

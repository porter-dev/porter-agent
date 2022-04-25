package utils

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/spf13/viper"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var porterHost string

type FilteredMessageResult struct {
	PodSummary        string
	PodDetails        string
	ContainerStatuses []*FilteredMessageContainerResult
}

type FilteredMessageContainerResult struct {
	ContainerName string
	Summary       string
	Details       string
}

type PodFilter interface {
	Filter(*corev1.Pod, bool) *FilteredMessageResult
}

type AgentPodFilter struct {
	kubeClient *kubernetes.Clientset
}

func init() {
	porterHost = viper.GetString("PORTER_HOST")
}

func NewAgentPodFilter(kubeClient *kubernetes.Clientset) PodFilter {
	return &AgentPodFilter{
		kubeClient: kubeClient,
	}
}

func (f *AgentPodFilter) Filter(pod *corev1.Pod, isJob bool) *FilteredMessageResult {
	res := &FilteredMessageResult{}

	for i := len(pod.Status.ContainerStatuses) - 1; i >= 0; i-- {
		status := pod.Status.ContainerStatuses[i]

		if isJob && (status.Name == "sidecar" || status.Name == "cloud-sql-proxy") {
			continue
		}

		scaleDownEvent := f.getContainerEventForReasons(pod.Name, pod.Namespace, status.Name, "ScaleDown")
		if scaleDownEvent != nil && strings.Contains(scaleDownEvent.Message, "deleting pod for node scale down") {
			continue
		}

		// if the exit code is 255, we check that the job doesn't have a different associated pod.
		// exit code 255 can mean this pod was moved to a different node due to node eviction, scaledown,
		// unhealthy node, etc
		if (status.State.Terminated != nil && status.State.Terminated.ExitCode == 255) ||
			(status.LastTerminationState.Terminated != nil && status.LastTerminationState.Terminated.ExitCode == 255) {
			pods, err := f.kubeClient.CoreV1().Pods(pod.Namespace).List(
				context.Background(), v1.ListOptions{
					LabelSelector: fmt.Sprintf("app.kubernetes.io/instance=%s", pod.Labels["app.kubernetes.io/instance"]),
				},
			)

			if err == nil && len(pods.Items) > 0 {
				shouldContinue := false

				for _, ownerPod := range pods.Items {
					if ownerPod.ObjectMeta.Name != pod.Name {
						shouldContinue = true
						break
					}
				}

				if shouldContinue {
					continue
				}
			}
		}

		containerResult := &FilteredMessageContainerResult{
			ContainerName: status.Name,
		}

		if status.State.Waiting != nil && status.State.Waiting.Reason != "" {
			if status.State.Waiting.Reason == "CrashLoopBackOff" {
				if status.LastTerminationState.Terminated != nil {
					if status.LastTerminationState.Terminated.Reason == "Error" {
						containerResult.Summary = fmt.Sprintf("The application exited with exit code %d",
							status.LastTerminationState.Terminated.ExitCode)

						if status.LastTerminationState.Terminated.ExitCode == 137 {
							// check for possible Killing or Unhealthy events for this container
							event := f.getContainerEventForReasons(
								pod.Name, pod.Namespace, status.Name, "Killing", "Unhealthy",
							)

							if event != nil {
								containerResult.Details = event.Message
							}
						}

						if containerResult.Details == "" {
							containerResult.Details = fmt.Sprintf("The application exited with exit code %d. "+
								"We recommend a resource such as https://intl.cloud.tencent.com/document/product/457/35758 "+
								"to further debug the reason for the crash.",
								status.LastTerminationState.Terminated.ExitCode)
						}
					} else if status.LastTerminationState.Terminated.Reason == "OOMKilled" {
						containerResult.Summary = "The application was killed because it used too much memory"
						containerResult.Details = fmt.Sprintf("The application exceeded its memory limit of %s. ",
							pod.Spec.Containers[0].Resources.Limits.Memory().String())

						containerResult.Details += "Reduce the amount of memory your application is using or increase the memory limit - " +
							"see the docs here for more information: https://docs.porter.run/managing-applications/" +
							"application-troubleshooting#memory-usage"
					} else if status.LastTerminationState.Terminated.Reason == "ContainerCannotRun" ||
						status.LastTerminationState.Terminated.Reason == "StartError" {
						containerResult.Summary = "The application could not start running"
						containerResult.Details = getFilteredMessage(status.LastTerminationState.Terminated.Message)
					}
				}
			} else if status.State.Waiting.Reason == "ErrImagePull" ||
				status.State.Waiting.Reason == "ImagePullBackOff" {
				containerResult.Summary = "The image could not be pulled from the registry"
				containerResult.Details = fmt.Sprintf("The application was unable to pull image %s. "+
					"Please make sure you have linked this image registry to Porter by navigating to %s/"+
					"integrations/registry. See documentation for linking your registry here: "+
					"https://docs.porter.run/deploying-applications/deploying-from-docker-registry/linking-existing-registry",
					status.Image, porterHost)
			} else if status.State.Waiting.Reason == "InvalidImageName" {
				containerResult.Summary = "The image could not be pulled from the registry because the image URI is invalid"
				containerResult.Details = fmt.Sprintf("The specified image %s is not a valid image URI.", status.Image)
			}
			// FIXME: check for RunContainerError
		} else if status.State.Terminated != nil && status.State.Terminated.Reason != "" {
			if status.State.Terminated.Reason == "Error" {
				containerResult.Summary = fmt.Sprintf("The application exited with exit code %d",
					status.State.Terminated.ExitCode)

				if status.State.Terminated.ExitCode == 137 {
					// check for possible Killing or Unhealthy events for this container
					event := f.getContainerEventForReasons(
						pod.Name, pod.Namespace, status.Name, "Killing", "Unhealthy",
					)

					if event != nil {
						containerResult.Details = event.Message
					}
				}

				if containerResult.Details == "" {
					containerResult.Details = fmt.Sprintf("The application exited with exit code %d. "+
						"We recommend a resource such as https://intl.cloud.tencent.com/document/product/457/35758 "+
						"to further debug the reason for the crash.",
						status.State.Terminated.ExitCode)
				}
			} else if status.State.Terminated.Reason == "OOMKilled" {
				containerResult.Summary = "The application was killed because it used too much memory"
				containerResult.Details = fmt.Sprintf("The application exceeded its memory limit of %s. ",
					pod.Spec.Containers[0].Resources.Limits.Memory().String())

				containerResult.Details += "Reduce the amount of memory your application is using or increase the memory limit - " +
					"see the docs here for more information: https://docs.porter.run/managing-applications/" +
					"application-troubleshooting#memory-usage"
			} else if status.State.Terminated.Reason == "ContainerCannotRun" ||
				status.State.Terminated.Reason == "StartError" {
				containerResult.Summary = "The application could not start running"
				containerResult.Details = getFilteredMessage(status.State.Terminated.Message)
			}
		} else if status.State.Terminated != nil && status.State.Terminated.Reason == "" {
			containerResult.Summary = fmt.Sprintf("The application exited with exit code %d",
				status.State.Terminated.ExitCode)
			containerResult.Details = fmt.Sprintf("The application exited with exit code %d. "+
				"We recommend a resource such as https://intl.cloud.tencent.com/document/product/457/35758 "+
				"to further debug the reason for the crash.",
				status.State.Terminated.ExitCode)
		}

		if containerResult.Details != "" && containerResult.Summary != "" {
			res.ContainerStatuses = append(res.ContainerStatuses, containerResult)
		}
	}

	if len(res.ContainerStatuses) == 0 {
		return nil
	} else if len(res.ContainerStatuses) == 1 {
		res.PodSummary = res.ContainerStatuses[0].Summary
		res.PodDetails = res.ContainerStatuses[0].Details
	} else { // more than one container
		summary := ""
		details := ""

		for _, containerResult := range res.ContainerStatuses {
			summary += fmt.Sprintf("Container: %s. Summary: %s\n", containerResult.ContainerName, containerResult.Summary)
			details += fmt.Sprintf("Container: %s. Details: %s\n", containerResult.ContainerName, containerResult.Details)
		}

		res.PodSummary = summary
		res.PodDetails = details
	}

	return res
}

func (f *AgentPodFilter) getContainerEventForReasons(
	podName, namespace, containerName string, reasons ...string,
) *corev1.Event {
	for _, reason := range reasons {
		events, err := f.kubeClient.CoreV1().Events(namespace).List(
			context.Background(), v1.ListOptions{
				FieldSelector: fmt.Sprintf(
					"involvedObject.name=%s,reason=%s,involvedObject.fieldPath=spec.containers{%s}",
					podName, reason, containerName),
			},
		)

		if err == nil && len(events.Items) > 0 {
			f.sortEventsByCreationTimestamp(events.Items)
			return events.Items[0].DeepCopy()
		}
	}

	return nil
}

func (f *AgentPodFilter) sortEventsByCreationTimestamp(events []corev1.Event) {
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].GetCreationTimestamp().After(events[j].GetCreationTimestamp().Time)
	})
}

func getFilteredMessage(message string) string {
	regex := regexp.MustCompile("starting container process caused:.*$")
	matches := regex.FindStringSubmatch(message)

	if len(matches) > 0 {
		return strings.TrimPrefix(matches[0], "starting container process caused: ")
	}

	return message
}

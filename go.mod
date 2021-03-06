module github.com/porter-dev/porter-agent

go 1.16

require (
	github.com/gin-gonic/gin v1.7.4
	github.com/go-logr/logr v0.3.0
	github.com/go-redis/redis/v8 v8.11.1
	github.com/google/uuid v1.3.0 // indirect
	github.com/onsi/ginkgo v1.15.0
	github.com/onsi/gomega v1.10.5
	github.com/spf13/viper v1.7.0
	k8s.io/api v0.20.2
	k8s.io/apimachinery v0.20.2
	k8s.io/client-go v0.20.2
	sigs.k8s.io/controller-runtime v0.8.3
)

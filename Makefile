# SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
#
# SPDX-License-Identifier: Apache-2.0

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.DEFAULT_GOAL := help

# NAME defines the name of the project, which is used in various targets for building and tagging images, as well as in the help output to provide context about the project.
NAME := redkey-robin

# VERSION defines the version of the project.
VERSION ?= 0.2.0


## Tool Versions and Configuration

# GOLANG_VERSION defines the Go version used in the Dockerfile for building the Robin controller image.
GOLANG_VERSION := 1.26.5

# REDIS_CLIENT_VERSION defines the version of the Redis client library used in the project. This variable is used in the Dockerfile to ensure that the correct version of the Redis client is included in the built image.
REDIS_CLIENT_VERSION := 8.10.0

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# OPERATOR_DIR defines the sibling operator checkout used by the local image build.
OPERATOR_DIR ?= ../redkey-operator

# OPERATOR_REPO and OPERATOR_REF locate the operator checkout that `ci-verify`
# materializes when one is not already present as a sibling directory.
OPERATOR_REPO ?= https://github.com/InditexTech/redkey-operator.git
OPERATOR_REF ?= main

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif


## Images configuration

# REGISTRY_NAME defines the name of the local registry container used for testing with Kind.
# This value is used in the Makefile to check if the registry container is running and to configure the
# local registry in the Kind cluster.
REGISTRY_NAME ?= kind-registry

# REGISTRY_PORT defines the port of the local registry used for testing with Kind. This value is used
# in the Makefile to construct the image tags for the bundle and catalog images, and to configure the
# local registry in the Kind cluster.
REGISTRY_PORT ?= 5005

# IMAGE_TAG_BASE defines the docker.io namespace and part of the image name for remote images.
# This variable is used to construct full image tags for bundle and catalog images.
#
# For example, running 'make bundle-build bundle-push catalog-build catalog-push' will build and push both
# inditex.dev/redkey-operator-bundle:$VERSION and inditex.dev/redkey-operator-catalog:$VERSION.
IMAGE_TAG_BASE ?= localhost:$(REGISTRY_PORT)/$(NAME)

# Image URL to use for building/pushing image targets.
IMG ?= $(IMAGE_TAG_BASE):$(VERSION)


.PHONY: all
all: build

##@ General

.PHONY: help
help: ##	Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ CI

.PHONY: verify
verify: tidy fmt vet lint build test-all ## Run all verification steps (fmt, vet, lint, unit and integration tests).

# `go.mod` replaces github.com/inditextech/redkey-operator with the sibling
# checkout at $(OPERATOR_DIR), so every Go command needs that directory to
# exist. Developers already have it; CI does not, so clone it first.
.PHONY: ci-verify
ci-verify: ## Run `verify` in CI, materializing the sibling operator checkout it depends on.
	@if [ ! -d "$(OPERATOR_DIR)" ]; then \
		echo "Cloning $(OPERATOR_REPO)@$(OPERATOR_REF) into $(OPERATOR_DIR)"; \
		git clone --depth 1 --branch "$(OPERATOR_REF)" "$(OPERATOR_REPO)" "$(OPERATOR_DIR)"; \
	fi
	$(MAKE) verify

.PHONY: version
version: ## Print the current version of the project.
	@echo "$(VERSION)"


##@ Development

.PHONY: tidy
tidy: ##	Run go mod tidy
	go mod tidy

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	$(GOLANGCI_LINT) config verify

# We use count=1 to disable test caching and force the tests to run every time.
.PHONY: test
test: fmt vet ## Run unit tests.
	go test $$(go list ./... | grep -v /e2e | grep -v /test/integration) -coverprofile cover.out -count=1

.PHONY: coverage
coverage: test ## HTML coverage from unit tests only.
	go tool cover -html=cover.out -o coverage.html

# We use count=1 to disable test caching and force the tests to run every time.
.PHONY: test-integration
test-integration: fmt vet setup-envtest ## Run integration tests (envtest).
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test ./test/integration/ -v -count=1

.PHONY: test-all
test-all: test test-integration ## Run all tests (unit + integration).

.PHONY: clean
clean: ## Clean de build artifacts, installed tools, Go cache and generated files.
	chmod -R u+w $(LOCALBIN) 2>/dev/null || true
	rm -rf bin
	rm -rf $(LOCALBIN)
	rm -rf cover.out coverage.html
	go clean --modcache


##@ Build

.PHONY: build
build: ##	Build program binary
	go build -o bin/robin cmd/main.go

# By default, Robin will be run with info log level. To run Robin with debug log level, set the LOG_DEBUG variable to true:
# - make run LOG_DEBUG=true
LOG_DEBUG ?= false

NAMESPACE ?= redkey-operator
CLUSTER_NAME ?= redkey-sample
.PHONY: run
run: ##	Execute the program locally
	go run ./cmd/main.go --cluster-name=$(CLUSTER_NAME) --namespace=$(NAMESPACE) $(if $(filter true,$(LOG_DEBUG)),--log-level=debug)

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: test-all ## Build docker image using a sibling operator checkout (uses `${IMG}` image name).
	DOCKER_BUILDKIT=1 $(CONTAINER_TOOL) build \
		--build-context redkey-operator=$(abspath $(OPERATOR_DIR)) \
		-t ${IMG} \
		--build-arg REDIS_CLIENT_VERSION=${REDIS_CLIENT_VERSION} \
		--build-arg GOLANG_VERSION=${GOLANG_VERSION} \
		--build-arg VERSION=$(VERSION) \
		--no-cache .

.PHONY: docker-push
docker-push: ##	Push docker image (uses `${IMG}` image name).
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/amd64,linux/arm64
.PHONY: docker-buildx
docker-buildx: test-all ## Build and push docker image for the manager for cross-platform support
	- $(CONTAINER_TOOL) buildx create --name redkey-robin-builder
	$(CONTAINER_TOOL) buildx use redkey-robin-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --annotation "index:org.opencontainers.image.description=Redkey Robin" --build-context redkey-operator=$(abspath $(OPERATOR_DIR)) --build-arg VERSION=$(VERSION) --tag ${IMG} --tag $(IMAGE_TAG_BASE):latest .
	- $(CONTAINER_TOOL) buildx rm redkey-robin-builder


##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell go list -m -f "{{ .Version }}" sigs.k8s.io/controller-runtime | awk -F'[v.]' '{printf "release-%d.%d", $$2, $$3}')
#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell go list -m -f "{{ .Version }}" k8s.io/api | awk -F'[v.]' '{printf "1.%d", $$3}')
GOLANGCI_LINT_VERSION ?= v2.1.0

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef

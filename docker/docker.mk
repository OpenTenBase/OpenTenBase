APP_NAME ?= opentenbase

# To use buildx: https://github.com/docker/buildx#docker-ce
export DOCKER_CLI_EXPERIMENTAL=enabled

# Debian APT mirror repository
DEBIAN_MIRROR ?=

# Docker image build and push setting
DOCKER:=DOCKER_BUILDKIT=1 docker
DOCKERFILE_DIR?=./docker

# BUILDX_PLATFORMS ?= $(subst -,/,$(ARCH))
BUILDX_PLATFORMS ?= linux/amd64,linux/arm64
BUILDX_ENABLED ?= true
BUILDX_BUILDER ?= "x-builder"

# Image URL to use all building/pushing image targets
IMG ?= docker.io/domainlau/$(APP_NAME)
VERSION ?= v2.5.0
TAG_LATEST ?= false

DOCKERFILE_DIR = ./docker
BUILD_ARGS ?=
DOCKER_BUILD_ARGS ?=

##@ Docker containers
.PHONY: build-image
build-image: DOCKER_BUILD_ARGS += --build-arg DEBIAN_MIRROR=$(DEBIAN_MIRROR)
build-image: install-docker-buildx
ifneq ($(BUILDX_ENABLED), true)
	$(DOCKER) build . $(DOCKER_BUILD_ARGS) --file $(DOCKERFILE_DIR)/Dockerfile --tag ${IMG}:${VERSION} --tag ${IMG}:latest
else
ifeq ($(TAG_LATEST), true)
	$(DOCKER) buildx build . $(DOCKER_BUILD_ARGS) --file $(DOCKERFILE_DIR)/Dockerfile --platform $(BUILDX_PLATFORMS) --tag ${IMG}:latest
else
	$(DOCKER) buildx build . $(DOCKER_BUILD_ARGS) --file $(DOCKERFILE_DIR)/Dockerfile --platform $(BUILDX_PLATFORMS)  --tag $(IMG):$(VERSION)
endif
endif

.PHONY: push-image
push-image: DOCKER_BUILD_ARGS += --build-arg DEBIAN_MIRROR=$(DEBIAN_MIRROR)
push-image: install-docker-buildx
ifneq ($(BUILDX_ENABLED), true)
ifeq ($(TAG_LATEST), true)
	$(DOCKER) push ${IMG}:latest
else
	$(DOCKER) push ${IMG}:${VERSION}
endif
else
ifeq ($(TAG_LATEST), true)
	$(DOCKER) buildx build . $(DOCKER_BUILD_ARGS) --file $(DOCKERFILE_DIR)/Dockerfile --platform $(BUILDX_PLATFORMS) --tag ${IMG}:latest --push
else
	$(DOCKER) buildx build . $(DOCKER_BUILD_ARGS) --file $(DOCKERFILE_DIR)/Dockerfile --platform $(BUILDX_PLATFORMS) --tag $(IMG):$(VERSION) --push
endif
endif

.PHONY: install-docker-buildx
install-docker-buildx: ## Create `docker buildx` builder.
	@if ! docker buildx inspect $(BUILDX_BUILDER) > /dev/null; then \
		echo "Buildx builder $(BUILDX_BUILDER) does not exist, creating..."; \
		docker buildx create --name=$(BUILDX_BUILDER) --use --driver=docker-container --platform linux/amd64,linux/arm64; \
	else \
		echo "Buildx builder $(BUILDX_BUILDER) already exists"; \
	fi

# Variables
SHELL := /bin/bash
ORGANIZATION := ecovoyage
DEFAULT_TAG := latest
DATE_TAG := $(shell date +%Y%m%d)

# Default Docker Buildx builder
BUILDER := default

# Path to the Docker Bake template and output file
STAGES := base core jupyter ai spatial testing devel
BAKE_TEMPLATE := docker-bake.template.hcl
BAKE_FILE := docker-bake.hcl

# Build task
.PHONY: docker-build
docker-build:
	# Generate the actual docker-bake.hcl from the template
	sed "s/ORG_PLACEHOLDER/$(ORGANIZATION)/g; s/DEFAULT_TAG_PLACEHOLDER/$(DEFAULT_TAG)/g; s/DATE_TAG_PLACEHOLDER/$(DATE_TAG)/g" $(BAKE_TEMPLATE) > $(BAKE_FILE)

	# Use the default Docker Buildx builder
	docker buildx use $(BUILDER)

	# Build the images using the generated Docker Bake file
	DOCKER_BUILDKIT=1 docker buildx bake -f $(BAKE_FILE)

# Push task
.PHONY: docker-push
docker-push:
	@$(foreach stage,$(STAGES), \
		docker push $(ORGANIZATION)/$(stage):$(DEFAULT_TAG); \
		docker push $(ORGANIZATION)/$(stage):$(DATE_TAG);)

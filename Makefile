# Variables
SHELL := /bin/bash
ORGANIZATION := ecovoyage
DEFAULT_TAG := latest
DATE_TAG := $(shell date +%Y%m%d)
STAGES := base core jupyter ai spatial testing devel

# Default Docker Buildx builder
BUILDER := default

BAKE_FILE := docker-bake.hcl

# Build task
build:
	@echo "group \"default\" {" > $(BAKE_FILE)
	@echo "  targets = [\"base\", \"core\", \"jupyter\", \"ai\", \"spatial\", \"testing\", \"devel\"]" >> $(BAKE_FILE)
	@echo "}" >> $(BAKE_FILE)
	$(foreach stage,$(STAGES), \
		echo "target \"$(stage)\" {" >> $(BAKE_FILE); \
		echo "  tags = [\"$(ORGANIZATION)/$(stage):$(DEFAULT_TAG)\", \"$(ORGANIZATION)/$(stage):$(DATE_TAG)\"]" >> $(BAKE_FILE); \
		if [ "$(stage)" != "base" ]; then echo "  inherit = [\"$$(echo $(STAGES) | cut -d' ' -f1-$$((i-1)))\"]" >> $(BAKE_FILE); fi; \
		echo "  target = \"$(stage)\"" >> $(BAKE_FILE); \
		echo "}" >> $(BAKE_FILE); \
		((i++));)
	DOCKER_BUILDKIT=1 docker buildx bake -f $(BAKE_FILE)

# Push task
push:
	@$(foreach stage,$(STAGES), \
		docker push $(ORGANIZATION)/$(stage):$(DEFAULT_TAG); \
		docker push $(ORGANIZATION)/$(stage):$(DATE_TAG);)

.PHONY: build push

# Copyright 2020 THL A29 Limited, a Tencent company.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


REGISTRY_NAME=xxx
IMAGE_TAGS=canary
LDFLAGS=$(shell bash version.sh)

CMDS=scheduler
all: build

build: build-scheduler

build-scheduler:
	go vet ./pkg/...
	mkdir -p bin
	GOOS=linux CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o ./bin/batch-scheduler ./cmd/scheduler

container-%: build-%
	docker build -t $(REGISTRY_NAME)/$*:$(IMAGE_TAGS) -f $(shell if [ -e ./cmd/$*/Dockerfile ]; then echo ./cmd/$*/Dockerfile; else echo Dockerfile; fi) --label revision=$(REV) .

push-%: container-%
	set -ex; \
	push_image () { \
		docker push $(REGISTRY_NAME)/$*:$(IMAGE_TAGS); \
	}; \
	for tag in $(IMAGE_TAGS); do \
		if [ "$$tag" = "canary" ] || echo "$$tag" | grep -q -e '-canary$$'; then \
			: "creating or overwriting canary image"; \
			push_image; \
		elif docker pull $(IMAGE_NAME):$$tag 2>&1 | tee /dev/stderr | grep -q "manifest for $(IMAGE_NAME):$$tag not found"; then \
			: "creating release image"; \
			push_image; \
		else \
			: "release image $(IMAGE_NAME):$$tag already exists, skipping push"; \
		fi; \
	done
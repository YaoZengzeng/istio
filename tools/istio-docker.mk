## Copyright 2018 Istio Authors
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.

.PHONY: docker
.PHONY: docker.all
.PHONY: docker.save
.PHONY: docker.push

### Docker commands ###
# Below provides various commands to build/push docker images.
# 使用各种命令构建/推送 docker images
# These are all wrappers around ./tools/docker, the binary that controls docker builds.
# 这些都是对于./tools/docker的封装，二进制控制docker builds
# Builds can also be done through direct ./tools/docker invocations.
# Builds也能通过直接调用./tools/docker完成
# When using these commands the flow is:
#  1) make target calls ./tools/docker
#  2) ./tools/docker calls `make build.docker.x` targets to compute the dependencies required
#  2) ./tools/docker调用`make build.docker.x` targets来计算需要的依赖
#  3) ./tools/docker triggers the actual docker commands required
#  3) ./tools/docker触发真正需要的docker命令
# As a result, there are two layers of make involved.

docker: ## Build all docker images
	./tools/docker

docker.save: ## Build docker images and save to tar.gz
	./tools/docker --save

docker.push: ## Build all docker images and push to
    # 构建所有的docker images并且推送
	./tools/docker --push

# Legacy command aliases
docker.all: docker
	@:
dockerx.save: docker.save
	@:
dockerx.push: docker.push
	@:
dockerx.pushx: docker.push
	@:
dockerx: docker
	@:

# Support individual images like `dockerx.pilot`

# Docker commands defines some convenience targets
# Build individual docker image and push it. Ex: push.docker.pilot
# 构建单独的docker image并且push它
push.docker.%:
	DOCKER_TARGETS=docker.$* ./tools/docker --push

# Build individual docker image and save it. Ex: tar.docker.pilot
tar.docker.%:
	DOCKER_TARGETS=docker.$* ./tools/docker --save

# Build individual docker image. Ex: docker.pilot
docker.%:
	DOCKER_TARGETS=docker.$* ./tools/docker

# Build individual docker image. Ex: dockerx.pilot
dockerx.docker.%:
	DOCKER_TARGETS=docker.$* ./tools/docker
### End docker commands ###

# Copyright (c) 2019 Platform9 Systems Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PF9_VERSION ?= 3.6.0
BUILD_NUMBER ?= 0
UID ?= $(shell id -u)
GID ?= $(shell id -g)
THISDIR = $(abspath $(dir $(CURDIR)))
BUILD = $(THISDIR)/../build
STAGE = $(THISDIR)/../build/stage
CENTOS_IMG ?= artifactory.platform9.horse/docker-local/py39-build-image:latest

all:
	docker run -i --rm -a stdout -a stderr \
		-u $(UID):$(GID) \
		-v $(THISDIR)/../:/src/pf9-ha \
		-w /src/pf9-ha/host/support \
		-e PF9_VERSION=$(PF9_VERSION) \
		-e BUILD_NUMBER=$(BUILD_NUMBER) \
		-e SIGN_PACKAGES=$(SIGN_PACKAGES) \
		$(CENTOS_IMG) \
		make wrapper

wrapper: rpm deb wrapperstage

rpm: rpmstage

deb: debstage

rpmstage: $(STAGE)/rpm

debstage: $(STAGE)/deb

wrapperstage: $(STAGE)/wrapper

$(STAGE)/wrapper:
	mkdir -p $@

wrapper: rpm deb
	$(CURDIR)/build-wrapper.sh $(PF9_VERSION) $(BUILD_NUMBER) $(BUILD) $(STAGE)/wrapper

rpm deb:
	$(CURDIR)/package.sh $(PF9_VERSION) $(BUILD_NUMBER) $(STAGE)/$@ $(BUILD) $@

$(BUILD):
	mkdir -p $@

$(STAGE)/rpm $(STAGE)/deb: $(BUILD)
	mkdir -p $@ && \
	    $(CURDIR)/install-python.sh $@ && \
	    pythonbin=$@/opt/pf9/python/bin/python && \
	    $(CURDIR)/install-vm-ha.sh $@ $${pythonbin} `basename $@`

clean:
	rm -rf $(BUILD)
	rm -rf $(STAGE)


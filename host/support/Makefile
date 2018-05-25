# Copyright (C) 2015 Platform 9 Systems, Inc.

PF9_VERSION ?= 3.6.0
BUILD_NUMBER ?= 0
UID ?= $(shell id -u)
GID ?= $(shell id -g)
SRCROOT = $(abspath $(dir $(CURDIR)))
BUILD = $(SRCROOT)/../build
STAGE = $(SRCROOT)/../stage

all: wrapper

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


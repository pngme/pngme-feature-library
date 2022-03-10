ROOTDIR := $(CURDIR)

.PHONY: format
format:
	black .

.PHONY: lint
lint:
	@for FEATUREDIR in lib/*; do \
		WORKDIR=$(ROOTDIR)/$$FEATUREDIR ; \
		echo "\nChecking: $$WORKDIR" ; \
		cd $$WORKDIR \
			&& black --check . \
			&& mypy . ; \
	done

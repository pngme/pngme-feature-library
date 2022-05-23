.PHONY: install
install:
	bash scripts/install.sh

.PHONY: format
format:
	.venv/bin/black .

.PHONY: lint
lint:
	bash scripts/lint.sh

.PHONY: test
test:
	bash scripts/test.sh

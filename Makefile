.PHONY: tests

TESTS := tests


test:
	@nosetests $(TESTS)

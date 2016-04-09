.PHONY: tests, clean

TESTS := tests

clean:
	@rm -rf *.png

test:
	@nosetests $(TESTS)

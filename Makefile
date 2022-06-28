.PHONY: test
test:
	PYTHONPATH=. 
	pytest -v

install:
	poetry install
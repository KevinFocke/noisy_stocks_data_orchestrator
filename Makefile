.PHONY: test
test:
	PYTHONPATH=. 
	pytest -v

install:
	poetry install

#TODO : Add ETL command + add mainflow command
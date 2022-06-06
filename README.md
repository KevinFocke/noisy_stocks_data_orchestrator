# Noisy Stocks

Generates spurious stock correlations based on historical stock price movers & shakers. 

Published online on [noisystocks.com](noisystocks.com).

## General information

This is a project-based learning experience. My main goals for the project:

* Organize complex project into extendable & easily understandable modules 

* Create automated data pipelines (ETL + Analysis + Publish)

	* Orchestrate data pipelines using Prefect flows
	* Parallelize processes using Dask
	* Extract datasets using Kaggle REST API 
	* Transform datasets into consistent & normalized Pandas DataFrame columns
	* Load datasets into local database (sqlite, expandable to other databases)
	* Query database for timerange
	* Calculate time-lagged autocorrelation across datasets
	* Export results to human-readable format for website publishing

* Practice advanced Test-Driven Development concepts (fixtures, API mocking)

* Enable continuous integration & continuous deployment in a production environment
* Make Python functions strongly typed using Pydantic & Pandera
* Integrate back-end & front-end securely (Jamstack)

## Installation
Requires;

- Poetry package manager; see required version in pyproject.toml.
- Linux environment with GNU make installed (often installed by default)
- Kaggle API key saved in kaggle.json for security: https://www.kaggle.com/docs/api



Install dependencies & create virtual Python environment:

	make install

## Usage Information

## Project Structure

## Reflection

## Credits

## Other
TODO: Create files for Black, Flake8 & isort settings
- Code formatting by Black (line-length 88):
https://black.readthedocs.io/en/stable/
- Linting using Flake8
- Imports sorted using isort
- Type checking using Pydantic & Pandera

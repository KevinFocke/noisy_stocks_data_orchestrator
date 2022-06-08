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
	* Load datasets into local database (PostgreSQL)
	* Query database for timerange
	* Calculate time-lagged autocorrelation across datasets
	* Export results to human-readable format for website publishing

* Practice advanced Test-Driven Development concepts (fixtures, API mocking)

* Enable continuous integration & continuous deployment in a production environment
* Make Python functions strongly typed using Pydantic & Pandera
* Integrate back-end & front-end securely (Jamstack)

## Installation
Requires;

- Linux OS with GNU make installed (often installed by default)
- Poetry see [pyproject.toml]
- Kaggle API key saved in kaggle.json for security: https://www.kaggle.com/docs/api



Install dependencies & create virtual Python environment:

	make install

## Usage Information

## Observability
Observe using Prefect Orion

	prefect orion start >> /workspace/logs/prefect.txt &


## Project Structure

## Reflection

## Credits

## Troubleshooting

### VSCode Development Environment
VSCode is handy as a cross-language IDE, but it often requires extra configuration. After much searching I've finally settled on this method.

Pros:
- Reprodicable evironment that persists even after container rebuild.
- Minimal cludge. No obtuse workarounds that easily break.
- No additional configuration required once set up.

Cons:
- Initial dependency installation can take several minutes

First, start up a Python 3.10 devcontainer. All commands should be run from within the container. Run:

	pip install poetry

Create env file in workspace: 

	poetry config virtualenvs.in-project true

Double check your settings:

	poetry config --list

Activate env:

	poetry shell

Upon initial install, restart VSCode. Select python interpreter. Selecting the correct interpreter can be further automated via VSCode tasks.json & launch.json.

## Other
TODO: Create files for Black, Flake8 & isort settings
- Code formatting by Black (line-length 88):
https://black.readthedocs.io/en/stable/
- Linting using Flake8
- Imports sorted using isort
- Type checking using Pydantic & Pandera

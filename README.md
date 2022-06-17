# Noisy Stocks

Generates spurious stock correlations based on historical stock price movers & shakers.

Published online on [noisystocks.com](noisystocks.com).

## General information

This is a project-based learning experience. My main goals for the project:

* Create a Cloud Native architecture (Container-Based + Dynamically-Scalable + Microservice-Oriented). The architecture is orchestrated by Kubernetes and uses pre-built and custom Docker images. The microservices are:
	* Ingestion (ETL) service for new datasets (on-demand)
	* Database service (PostgreSQL-based)
	* Analysis service (Custom-written Python, parallellizing Pandas DataFrames using Dask). 
	* Publishing service (custom Python + static site generation using Hugo)
	* Logging service for Python flows using Prefect 2.0 (Orion).
	* Logging service for communication observability between different microservices

* Setup FastAPI communication accross different microservices

* Enable continuous integration & continuous deployment in a production environment

* Practice advanced Python Test-Driven Development concepts (fixtures, API mocking)

* Make Python functions strongly typed using Pydantic & Pandera

* Integrate back-end & front-end securely using Jamstack.

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

Double check your settings:

	poetry config --list

Activate env:

	poetry shell

Upon initial install, restart VSCode & select python interpreter. Selecting the correct interpreter can be further automated via VSCode tasks.json & launch.json.

## Other
TODO: Create files for Black, Flake8 & isort settings
- Code formatting by Black (line-length 88):
https://black.readthedocs.io/en/stable/
- Linting using Flake8
- Imports sorted using isort
- Type checking using Pydantic & Pandera

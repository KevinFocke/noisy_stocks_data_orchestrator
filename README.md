# Noisy Stocks

Generates spurious stock correlations based on historical stock price movers & shakers.

Published online on [noisystocks.com](noisystocks.com).

## General information

This is a project-based learning experience. My main goals for the project:

* Create a Cloud Native architecture (Container-Based + Dynamically-Scalable + Microservice-Oriented). The microservices are:
	* Database service (Timescale, PostgresQL-based specialized for time series)
	* Analysis service (Custom Python, parallellizing Pandas DataFrames using Dask)
	* Publishing service (Custom Python + static site generation using Hugo)
	* Data Orchestrator (Prefect 2.0)
	* Worker agent(s)

Features:
* Automatically finds correlations based on provided time series

* Gracefully recovers from failure to minimize service interruptions

* Integrates back-end & front-end securely using Jamstack (Static Site Generation).

* Observability into every aspect of the architecture

* Strongly typed Python using Pydantic & Pandera to increase reliability

* Optimized database with compression rates up to 80%+

* Continuous integration & continuous deployment in a production environment


Planned for version 1.5:

* Add Content Management System (CMS) using a custom CLI interface


## Installation

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

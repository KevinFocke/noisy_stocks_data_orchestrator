# Noisy Stocks

Generates spurious stock correlations based on historical stock price movers & shakers.

Published online on [noisystocks.com](noisystocks.com).

## General information

My main goal was to create a Cloud Native architecture (Container-Based + Dynamically-Scalable + Microservice-Oriented). Containers run on Docker. Agent(s) dynamically pick up available work. The microservices are:

* Database service (Timescale; PostgresQL specialized for time series)
* Analysis service (Custom Python 3, parallellizing Pandas DataFrames using Dask)
* Publishing service (Custom Python + Static Site Generation using Hugo)
* Data Orchestrator (Prefect 2.0)
* Worker agent(s)

Features:

* Automatically finds correlations based on time series

* Optimized Big Data database
	- more than 870 million weather & 14 million stock datapoints
	- 90+% compression rate

* Gracefully recovers from failure to minimize service interruptions

* Integrates back-end & front-end securely using Jamstack architecture

* Observability into every aspect of the architecture

* Strongly typed Python using Pydantic & Pandera to increase reliability

* DevOps best practices: 
	* Continuous Integration
	* Continuous Deployment
	* Reproducable development environment


Considerations:

* During development I became fascinated with the potential of Kubernetes. However, for this particular application, Kubernetes was not the best fit; the resource overhead & intrinsic complexity outweighs the benefits. In the future I do intend to explore Kubernetes more.

Planned for version 1.5:

* Add Content Management System (CMS) using a custom CLI interface

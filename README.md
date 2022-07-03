# Noisy Stocks

Generates spurious stock correlations based on historical stock price movers & shakers.

Published online on [noisystocks.com](noisystocks.com).

## General information

My main goal was to create a Cloud Native architecture (Container-Based + Dynamically-Scalable + Microservice-Oriented). Containers run on Docker. An agent dynamically picks up available work. The services are:


* Analysis service (Custom Python 3 using Pandas DataFrames)
* Publishing service (Custom Python 3)
* Static Site Generation service (Hugo)
* Data Pipeline Orchestrator (Prefect 2.0)
* Worker agents

Each service has a separate database. (Timescale; PostgresQL specialized for time series).

Features:

* Automatically finds correlations based on time series

* Optimized Big Data database
	- more than 870 million weather & 14 million stock datapoints
	- 90+% compression rate

* Decouples back-end & front-end securely using Jamstack architecture.

* Strongly typed Python using Pydantic & Pandera to increase reliability

* DevOps best practices: 
	* Continuous Integration
	* Continuous Deployment
	* Reproducable development environment


Considerations:

* During development I became fascinated with the potential of Kubernetes. However, high availability architectures, such as provided by Kubernetes, add complexity & resource overhead. In my case, high availability is not a critical requirement for the back-end services. Why? Because the uptime of the website is NOT impacted by downtime of the back-end services. If the back-end services are down it will only affect the freshness of the content.

In the future I do intend to explore Kubernetes more.

Reflection:

* It is challenging to decide on the _right_ size of a microservice.
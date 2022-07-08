# Noisy Stocks

Generates spurious stock correlations based on historical stock price movers & shakers.

Published online on [noisystocks.com](noisystocks.com).

## General information

My main goal was to create a Cloud Native architecture (Container-Based + Dynamically-Scalable + Microservice-Oriented). Containers run on Docker. Agents dynamically pick up available work. The services are:


* Analysis service (Custom Python 3 using Pandas DataFrames)
* Publishing service (Custom Python 3)
* Static Site Generation service (Hugo)
* Data Pipeline Orchestrator (Prefect 2.0)
* Worker agents

Databases are PostgresQL specialized for time series (Timescale)

Features:

# TODO: add speed test result

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


# Requirements
 
There were a couple interesting problems that needed to be solved by the program:

- Time series data is intrinsically challenging because a calendar year (365 days + leap day roughly every 4 years) does not align exactly with an astronomical year on earth (365.2422 days, source: https://pumas.nasa.gov/sites/default/files/examples/04_21_97_1.pdf). Furthermore, you need to take into account timezones.
- When you correlate two time series datasets, how do you ensure the time series data is aligned? How do you handle missing datapoints?
- Financial markets can be closed due to the weekend or holidays. If you go back an arbitrary amount of days, how can you get the most usable time series range selection?


# Limitations:

## Encapsulation
To maintain a good overview, functionality should be enclosed in subfunctions. For most functions, I was able to enclose them. However, the Prefect 2.0 beta orchestrator seems to face issues with passing some kinds of arguments (eg. a sqlalchemy engine, and a pickle containing deeply nested dictionaries & dataframes). It kept throwing all kinds of cryptic errors. The temporary workaround was to simply inline the functionality. Not ideal, but for now it works. 

Prefect 2.0 beta also seems to make the implicit assumption that any subflow should be cloudpickled & materialized.

## Database Representation
Currently, a datapoint has to be uniquely identified by a (longitude, latitude) composite key. However, for maximum flexibility, the program should also be able to uniquely identify datapoints which are not geocoordinates on earth. In principle, any numeric column of a dataset could be linked to a numeric col of a stock. 

Solved sub-problems:
- The Python correlation program is already adapted to compare any arbitrary numeric columns between two datasets. It can also handle one (or multiple) unique identifiers of the dataset during analysis and sql export.
- The database will change over time. How can you ensure that the results are reproducable?
	- Solution: Materialize all relevant export info and save the filename + hash in the database. The materialization of the final result is remarkably small (<1kb per correlation pair on a 5-day time range)

Unsolved sub-problems:
- In a database, how can you uniquely represent an entry of a dataset which might be composed of one, or multiple keys?
eg. (longitude, latitude) composite key, or (planet_name, longitude, latitude)

# Considerations:

* To keep the performance of queries reasonable, there should be one table per Big Data dataset. Each table requires a minimum of 3 fields:
	* timestamp with timezone
	* uid (eg. composite key made of longitude + latitude)
	* float value to correlate

* During development I became fascinated with the potential of Kubernetes. However, high availability architectures, such as provided by Kubernetes, add complexity & resource overhead. In my case, high availability is not a critical requirement for the back-end services. Why? Because the uptime of the website is NOT impacted by downtime of the back-end services. If the back-end services are down it will only affect the freshness of the content.

In the future I do intend to explore Kubernetes more.


# Reflection

* It is challenging to decide on the _right_ size of a microservice.

# Troubleshoot


Is Python dev tools binary installed for your specific python version? Debian-based system command:

	
	sudo apt-get install python3.10-dev
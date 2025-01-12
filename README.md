# Car-Rental-Batch-Ingestion-Project

Project Overview

The Car Rental Batch Ingestion Project automates the processing and ingestion of car rental data into a Snowflake data warehouse. It utilizes Apache Airflow for orchestration, Google Cloud Storage (GCS) for raw data storage, Google Dataproc for data processing, and Snowflake for data warehousing.

## Project Architecture
![Architecture Overview](https://github.com/Kaushik-Puttaswamy/Car-Rental-Analytics-Pipeline-with-Airflow-Snowflake-and-GCP/blob/main/Architecture%20Overview.png)

## Key Components and Workflow

1. Airflow DAG:
   
	• The DAG (car_rental_airflow_dag.py) orchestrates the data pipeline.

	•	Key tasks:

	  •	Extract the execution_date from parameters.
   
	  •	Merge and insert data into Snowflake for the customer_dim table.
   
	  •	Submit a PySpark job to Dataproc for processing raw rental data.
   
  •	DAG Graph:
  
![Airflow_dag_graph.png](https://github.com/Kaushik-Puttaswamy/Car-Rental-Analytics-Pipeline-with-Airflow-Snowflake-and-GCP/blob/main/Airflow_dag_graph.png)

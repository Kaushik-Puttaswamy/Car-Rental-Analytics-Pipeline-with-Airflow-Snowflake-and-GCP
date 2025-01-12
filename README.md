# Car-Rental-Analytics-Pipeline-with-Airflow-Snowflake-and-GCP

## Project Overview

The Car Rental Batch Ingestion Project automates the processing and ingestion of car rental data into a Snowflake data warehouse. It utilizes Apache Airflow for orchestration, Google Cloud Storage (GCS) for raw data storage, Google Dataproc for data processing, and Snowflake for data warehousing.

## Project Architecture
![Architecture Overview](https://github.com/Kaushik-Puttaswamy/Car-Rental-Analytics-Pipeline-with-Airflow-Snowflake-and-GCP/blob/main/Architecture%20Overview.png)

## Key Components and Workflow

### 1. Airflow DAG:
   
 • The DAG (car_rental_airflow_dag.py) orchestrates the data pipeline.

Key tasks:

   • Extract the execution_date from parameters.
   
   • Merge and insert data into Snowflake for the customer_dim table.
   
   • Submit a PySpark job to Dataproc for processing raw rental data.
   
   
• DAG Graph:
  
![Airflow_dag_graph.png](https://github.com/Kaushik-Puttaswamy/Car-Rental-Analytics-Pipeline-with-Airflow-Snowflake-and-GCP/blob/main/Airflow_dag_graph.png)

### 2. Data Processing with PySpark:
   
The PySpark script (spark_job.py) performs the following:

• Reads JSON data from GCS.

• Validates and transforms the data (e.g., calculates rental durations, amounts).

• Joins the raw data with Snowflake dimension tables (car_dim, location_dim, date_dim, customer_dim).

• Writes the processed data to the rentals_fact table in Snowflake.

### 3. Google Dataproc Cluster:

• A Dataproc cluster is used to run the PySpark job.

• Cluster details:

![Dataproc_cluster_details](https://github.com/Kaushik-Puttaswamy/Car-Rental-Analytics-Pipeline-with-Airflow-Snowflake-and-GCP/blob/main/Dataproc_cluster_details.png)

### 4. Snowflake Data Warehouse:
   
• Dimension and fact tables are designed for analytics:

   • location_dim, car_dim, date_dim, customer_dim (dimensions).

   • rentals_fact (fact table).

• Tables are populated using Snowflake SQL scripts (snowflake_dwh_setup.sql).

### 5. Storage:

GCS is used for raw data storage:
 
   • Daily customer and rental data are stored in CSV and JSON formats.
   
Example data files:
 
   • car_rental_20240803.json
   
   • customers_20240803.csv

## Folder and File Structure

### Airflow DAG

car_rental_airflow_dag.py: Defines the Airflow pipeline.

### PySpark Job

 •  spark_job.py: Processes raw data and integrates with Snowflake.

### Data Files

 •  mock_data/: Contains raw data files (JSON/CSV).

### Snowflake SQL

 •  snowflake_dwh_setup.sql: Creates dimension and fact tables in Snowflake.

### Jar Files

jar_files/: Links to required JAR files for Snowflake connectivity:

 •  spark-snowflake JAR: https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.15.0-spark_3.4/spark-snowflake_2.12-2.15.0-spark_3.4.jar

 •  snowflake-jdbc JAR: https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.16.0/snowflake-jdbc-3.16.0.jar

### Pipeline Workflow

### 1.	Data Upload:
	
   • Raw customer and rental data are uploaded to GCS.
 
### 2.	Airflow Execution:
	
   • Airflow orchestrates the pipeline:
 
   1.	Extracts the execution_date.
  
   2.	Updates the customer_dim table using Snowflake SQL tasks.

   3.	Triggers the PySpark job in Dataproc.

### 3.	PySpark Processing:

• Reads raw data from GCS.
• Joins raw data with Snowflake dimension tables.
• Writes the processed fact data back to Snowflake.
 
### 4.	Snowflake Analytics:

• Data is available in Snowflake for querying and analysis.

#### Key project screenshots:

1.	Airflow Buckets and DAG Files:

   ![Airflow_buckets_dag_files](https://github.com/Kaushik-Puttaswamy/Car-Rental-Analytics-Pipeline-with-Airflow-Snowflake-and-GCP/blob/main/Airflow_buckets_dag_files.png)

2.	Snowflake Buckets and Folders:

![Snowflake_Buckets_carrental_folders](https://github.com/Kaushik-Puttaswamy/Car-Rental-Analytics-Pipeline-with-Airflow-Snowflake-and-GCP/blob/main/Snowflake_Buckets_carrental_folders.png)

3.	YARN Resource Manager:
   
   ![Spark_job_YARN_resource_manager](https://github.com/Kaushik-Puttaswamy/Car-Rental-Analytics-Pipeline-with-Airflow-Snowflake-and-GCP/blob/main/Spark_job_YARN_resource_manager.png)
   
4. Airflow-Snowflake Connection:
   
![airflow_snowflake_connection](https://github.com/Kaushik-Puttaswamy/Car-Rental-Analytics-Pipeline-with-Airflow-Snowflake-and-GCP/blob/main/airflow_snowflake_connection.png)

#### How to Run the Project

1. Set Up Snowflake:
   
	•	Execute snowflake_dwh_setup.sql to create tables and stage in Snowflake.

2. Upload Data:
   
	•	Upload the mock data files to GCS.

3.	Start Airflow:
   
	•	Trigger the DAG (car_rental_data_pipeline) in Airflow.

4.	Monitor Processing:
   
	•	Check the Dataproc cluster and Airflow UI for task progress.

5.	Analyze in Snowflake:
   
	•	Query the dimension and fact tables for insights.

#### Additional Information

1) Error Handling:
 
   • Airflow retries failed tasks with exponential backoff.
    
   • PySpark job validates data to prevent invalid records from propagating.
    
2) Scalability:
 
   • Dataproc handles large-scale data processing.
    
   • Snowflake supports efficient querying for massive datasets.

#### Future Enhancements

  • Automate the DAG trigger to run daily based on file availability.

  • Add monitoring and alerting for failed tasks.

  • Optimize PySpark transformations for large datasets.

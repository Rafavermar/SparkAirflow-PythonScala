# Spark and Airflow Integration for Submitting Python and Scala Jobs
![Project_architecture.png](Assets/Project_architecture.png)

## Introduction
This project orchestrates Spark jobs written in different programming languages using Apache Airflow, all within a Dockerized environment. The DAG `sparking_flow` is designed to submit Spark jobs written in Python, Scala, and Java, ensuring that data processing is handled efficiently and reliably on a daily schedule.

## Project Structure
The DAG `sparking_flow` includes the following tasks:
- **start**: A `PythonOperator` that prints "Jobs started".
- **python_job**: A `SparkSubmitOperator` that submits a Python Spark job.
- **scala_job**: A `SparkSubmitOperator` that submits a Scala Spark job.
- **java_job**: A `SparkSubmitOperator` that submits a Java Spark job.
- **end**: A `PythonOperator` that prints "Jobs completed successfully".

These tasks are executed in a sequence where the start task triggers the Spark jobs in parallel, and upon their completion, the end task is executed.

## Prerequisites
Before setting up the project, ensure you have the following:
- Docker and Docker Compose installed on your system.
- Apache Airflow Docker image or a custom image with Airflow installed.
- Apache Spark Docker image or a custom image with Spark installed and configured to work with Airflow.
- Docker volumes for Airflow DAGs, logs, and Spark jobs are properly set up.

## Docker Setup
To run this project using Docker, follow these steps:
1. Clone this repository to your local machine.
2. Navigate to the directory containing the `docker-compose.yml` file.
3. Build and run the containers using Docker Compose:
    ```bash
    docker-compose up -d --build
    ```
    This command will start the necessary services defined in your `docker-compose.yml`, such as Airflow webserver, scheduler, Spark master, and worker containers.

## Directory Structure for Jobs
Ensure your Spark job files are placed in the following directories and are accessible to the Airflow container:
- **Python job**: `jobs/python/wordcountjob.py`
- **Scala job**: `jobs/scala/target/scala-2.12/word-count_2.12-0.1.jar`
- **Java job**: `jobs/java/spark-job/target/spark-job-1.0-SNAPSHOT.jar`

These paths should be relative to the mounted Docker volume for Airflow DAGs.

## Usage
After the Docker environment is set up, the `sparking_flow` DAG will be available in the Airflow web UI at `localhost:8080`, where it can be triggered manually or run on its daily schedule.

The DAG will execute the following steps:
1. Print "Jobs started" in the Airflow logs.
2. Submit the Python Spark job to the Spark cluster.
3. Submit the Scala Spark job to the Spark cluster.
4. Submit the Java Spark job to the Spark cluster.
5. Print "Jobs completed successfully" in the Airflow logs after all jobs have finished.

### Important Note
When configuring the connection to Spark in the Airflow UI, the Spark master must be specified as `spark://spark-master`.


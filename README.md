## DBLP-ETL-Pipeline
This is an automated pipeline for processing data from the DBLP computer science bibliography, storing it in a Cassandra database, and performing basic analytics on the data. The pipeline is implemented using Apache Airflow, Python, and Cassandra.

Feel free to view the project document to see the data piepline's design rationales and efficiencies 

# Getting Started
Prerequisites
To run the pipeline, you will need the following installed:

1. Apache Airflow
2. Cassandra
3. Python 3.x

# Installation
1. Clone this repository to your local machine.
2. Navigate to the project directory in your terminal.
3. Run pip install -r requirements.txt to install the required Python packages.
4. Configure Apache Airflow by setting the AIRFLOW_HOME environment variable to a directory where you would like to store Airflow's configuration files and database. 
5. You can do this by adding the following line to your .bashrc or .bash_profile file: `export AIRFLOW_HOME=/path/to/airflow/home`
6. `airflow initdb`
7. Start the Airflow web server and scheduler by running the following command:
`airflow webserver -p 8080`
`airflow scheduler`
8. Open your web browser and navigate to http://localhost:8080/ to access the Airflow web UI.


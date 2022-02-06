# databricks-with-airflow
manage databricks workspace, cluster and jobs through airflow

# Running Airflow Instance locally
clone the repository and start the docker containers. If running in MacOS do consider increasing the dedicated memory to at least 4 GB for running airflow containers. Base airflow docker image: https://hub.docker.com/r/apache/airflow
## initialise airflow
* > docker-compose build
* > docker-compose up airflow-init  # that helps in initializing the airflow db. It sets username - 'airflow' and password - 'airflow'
## run airflow instance
* > docker-compose up

# Submit job to databricks using default databricks connection and Personal Access Token (PAT)
Create PAT which authorizes to connect with databricks instance.
* create airflow connection with 'connection_id' databricks_default which is the default connection name to connect with databricks
* select databricks as connection type
* set host as "https://[databricks-instance-url]/
* add PAT in extras, eg.  {"token":"[Personal access token]"}
  
Change the **databricks_cluster.py** according to your databricks setup, here I have used existing cluster and notebook. You can refere https://docs.databricks.com/dev-tools/api/2.0/jobs.html#runs-submit. 

And you are ready to test your first dag. 

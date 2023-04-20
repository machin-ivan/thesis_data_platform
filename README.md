## Thesis project


First go to airflow directory and open .env file. 
Then set AIRFLOW_UID with ID of your user (run "echo $UID" command in your cmd to get it). 

Project setup:

- go to the project folder in your cmd
- run the run_docker.sh script
- when containers are up and healthy, run db_setup.sh
- next open localhost:8080 in your browser and log into Airflow web client with parameters:
    * login: airflow
    * password: airflow





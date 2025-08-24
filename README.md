# Springboard Project 3: Apache Airflow
 Use Apache Airflow to create a data pipeline to extract online stock market  data and deliver analytical results. 
Note: This README file assumes that docker and apache airflow is correctly configured on user's end.

# Pre-requisites
- Docker Desktop
- VS Code
- Download this directory

# Steps to run
1. Start Docker Desktop
   Make sure Docker is running.
2. Open Project Folder. Open this directory in VS Code or a terminal.
   ```
   # This can be run on a command prompt --This should open VS Code with this directory
   this/directory/path code .
   ```
3. Start Airflow
   In VS Code terminal, run:
   ```
   docker-compose up airflow-apiserver airflow-scheduler
   ```
4. Open Airflow UI by going to: http://localhost:8080
   Login
   - Username: airflow
   - Password: airflow
 
5. Run the marketvol DAG.
   On the UI, make sure that the marketvol DAG in unpaused.
   Note that the python code this is based on is springboard_project3.py
   <img width="1712" height="784" alt="image" src="https://github.com/user-attachments/assets/5f59ed15-c4bd-4672-b73b-f5fc0767032d" />

When DAG successfully executes on its scheduled time, expect to find the results within this project directory under:
/dags/output/<YYYY-MM-DD>

<img width="374" height="528" alt="image" src="https://github.com/user-attachments/assets/7a198d86-2947-4795-8adb-8db64d09f1d4" />



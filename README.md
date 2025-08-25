# Springboard Project 3: Apache Airflow
 Use Apache Airflow to create a data pipeline to extract online stock market  data and deliver analytical results. 
Note: This README file assumes that docker and apache airflow is correctly configured on user's end.

# Pre-requisites
- Docker Desktop
- VS Code
- Download this directory. Under dags folder, the DAG is written in springboard_project3.py

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
4. Open Airflow UI by going to: http://localhost:8080.
   Login
   - Username: airflow
   - Password: airflow
   
   If UI shows up but DAGs are not showing, you may need to type ```docker-compose down``` then ```docker-compose up``` in the terminal. 
5. Run the marketvol DAG.
   On the UI, make sure that the marketvol DAG in unpaused.
   Note that the python code this is based on is springboard_project3.py
   <img width="1712" height="784" alt="image" src="https://github.com/user-attachments/assets/5f59ed15-c4bd-4672-b73b-f5fc0767032d" />


Once the DAG has executed its tasks as scheduled, the UI will look like this for marketvol
<img width="1218" height="662" alt="image" src="https://github.com/user-attachments/assets/9a491849-cbb0-40c3-94e3-56a04b73df28" />

If the DAG successfully executes, expect to find the results within this project directory under:
/dags/output/<YYYY-MM-DD>. Note that this folder is created in t5, so this folder will appear once the tasks have been executed.

<img width="1052" height="577" alt="image" src="https://github.com/user-attachments/assets/4dcf5277-ae6e-46b1-b006-1cac87f2a9a0" />




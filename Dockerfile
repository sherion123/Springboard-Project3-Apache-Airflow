FROM apache/airflow:3.0.4
COPY requirement.txt /requirement.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirement.txt
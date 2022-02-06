FROM apache/airflow:2.2.3
USER airflow
RUN mkdir /home/airflow/app
WORKDIR /home/airflow/app
COPY ./requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt
ENV PYTHONUNBUFFERED 1

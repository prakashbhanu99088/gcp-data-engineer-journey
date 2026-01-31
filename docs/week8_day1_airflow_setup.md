\# Week 8 Day 1 — Airflow Setup + First DAG



\## Goal

Set up Apache Airflow locally with Docker and run a first DAG.



\## Steps completed

1\. Started Airflow with Docker Compose

2\. Opened Airflow UI on http://localhost:8080

3\. Created a DAG: `hello\_airflow`

4\. Ran the DAG manually and verified logs



\## Commands used

\- Start containers:

&nbsp; docker compose up -d

\- Check status:

&nbsp; docker compose ps

\- Restart scheduler (if DAG not visible):

&nbsp; docker compose restart scheduler



\## Result

Airflow is running correctly and can detect DAG files from `airflow/dags/`.




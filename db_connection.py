import datetime
import logging
import cx_Oracle
import sys
import csv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
#from create_tables import create_table_bestandsload_nolo

conn = psycopg2.connect("host=database-2.cf8dbsgfgk0h.us-west-2.rds.amazonaws.com port=5432 user=postgres password=alexanderdudko dbname=dwh2")
cur = conn.cursor()

def conn_oracle():
    logging.info("create connection to Oracle Database")
    try:
        conn = psycopg2.connect("host=database-2.cf8dbsgfgk0h.us-west-2.rds.amazonaws.com port=5432 user=postgres password=alexanderdudko dbname=dwh2")
        # conn = cx_Oracle.connect('STG01/oaoadmin@BDOPIS1/DWH1')
        print("1. Connected to Postgres")
    except psycopg2.Error as e:
        print("Error connect to Postgres")
        print(e)



def create_cursor():
    try:
        cur = conn.cursor()
        logging.info("2. Cursor Created")
    except psycopg2.Error as e:
        logging.info("Error with cursor")
        print(e)

cur = conn.cursor()



"""
def create_tables():
    try:
        cur.execute(create_table_bestandsload_nolo)
        conn.commit()
        print("table created")
    except cx_Oracle.Error as e:
        print("Error creating table")
        print(e)
"""



def read_nolo_csv():
    try:
        with open('/root/airflow/dags/etl_as_code/input/nolo/bestand_00526778.csv', 'r') as f:
            reader = csv.reader(f, delimiter=';') #(f, delimiter=';')
            next(reader) # skip the header row
            for row in reader:
                print(row)
                #logging.info(print(row))
                cur.execute(
                "INSERT INTO public.BESTANDSLOAD_NOLO (menge, artikelnr, LAGERORT, RESERVIERTEMENGE, LAGERSTANDORT, MENGEFUERAUFTRAEGE) VALUES (%s, %s, %s, %s, %s, %s)", row)
            conn.commit()
            print("success inserting")
    except cx_Oracle.Error as e:
        print("Error inserting")
        print(e)



def read_nolo_share_csv():
    try:
        with open('/root/airflow/dags/etl_as_code/input/nolo/bestand_00526778.csv', 'r') as f:
            reader = csv.reader(f, delimiter=';') #(f, delimiter=';')
            next(reader) # skip the header row
            for row in reader:
                print(row)
                #logging.info(print(row))
                cur.execute(
                "INSERT INTO public.BESTANDSLOAD_NOLO (menge, artikelnr, LAGERORT, RESERVIERTEMENGE, LAGERSTANDORT, MENGEFUERAUFTRAEGE) VALUES (%s, %s, %s, %s, %s, %s)", row)
            conn.commit()
            print("success inserting")
    except cx_Oracle.Error as e:
        print("Error inserting")
        print(e)


# (:1, :2, :3, :4, :5, :6)
# (%s, %s, %s, %s, %s, %s)
 #(artikelnr, LAGERORT, RESERVIERTEMENGE, LAGERSTANDORT, MENGEFUERAUFTRAEGE, MENGE)


dag = DAG(
        'bestands_load',
        start_date=datetime.datetime.now() - datetime.timedelta(days=1))

conn_task = PythonOperator(
    task_id="Verbindung zu Postgres-DB",
    python_callable=conn_oracle,
    dag=dag
)

cursor_task = PythonOperator(
        task_id="Erstelle Cursor zur Postgres-DB",
        python_callable=create_cursor,
        dag=dag
        )

read_nolo_csv_task = PythonOperator(
        task_id="Lese die Nolo.csv ein und schreibe diese in die Tabelle bestandsload_nolo",
        python_callable=read_nolo_csv,
        dag=dag
        )

read_nolo_share_csv_task = PythonOperator(
        task_id="Lese die Nolo_share.csv ein und schreibe diese in die Tabelle bestandsload_nolo",
        python_callable=read_nolo_share_csv,
        dag=dag
        )

conn_task >> cursor_task
cursor_task >> read_nolo_csv_task
cursor_task >> read_nolo_share_csv_task

#conn_oracle()
#create_cursor()
#create_tables()
#read_write_csv_to_db()

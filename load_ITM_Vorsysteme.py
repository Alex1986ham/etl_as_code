# -*- coding: utf-8 -*-

import datetime
import logging
import cx_Oracle
import sys
import csv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Roedl conn = cx_Oracle.connect('DBTeam/DDfO10g@10.107.30.127/BMD_OPISDWH')
conn = cx_Oracle.connect('STG01/oaoadmin@BDOPIS1/DWH1') # Keller
cur = conn.cursor()
# Create connection to Oracle Database

def conn_oracle():
    try:
        conn = cx_Oracle.connect('STG01/oaoadmin@BDOPIS1/DWH1') # Keller
        print("1. Connected to Oracle")
    except cx_Oracle.Error as e:
        print("Error")
        print(e)


# Craete Cursor
def create_cursor():
    try:
        cur = conn.cursor()
        print("2. Cursor created")
    except cx_Oracle.Error as e:
        print("Error")
        print(e)


def delete_ODS_KERN_TMP_MMDB_STYLE():
    try:
        cur.execute("DELETE FROM ODS.KERN_TMP_MMDB_STYLE")
        conn.commit()
        print("delete successful")
    except cx_Oracle.Error as e:
        print("Error deleting")
        print(e)



def read_MMDB_csv_write_db():
    try:
        with open('/mnt/I/obiatotto/_Übergreifende Themen und Projekte/OPIS/Schnittstelle/Schnittstellen_Templates_endgueltig/APEX_MMDB/MMDB.csv', 'r') as f:
            reader = csv.reader(f, delimiter=';')
            included_cols = [1, 0, 2, 6, 7, 8]  #  [2, 1, 3, 7, 5, 8, 9]
            next(reader) # skip the header row
            for row in reader:
                content = list(row[i] for i in included_cols)
                print(content)
                cur.execute(
                "INSERT INTO ODS.KERN_TMP_MMDB_STYLE (ART_NR, STYLE_NR, STYLENAME, SAISON, MERCHANDSISESTYLECOMMKEY, ART_COMKEY) VALUES (:1, :2, :3, :4, :6, :7)", content)
                conn.commit()
            print("success inserting")
    except cx_Oracle.Error as e:
        print("Error inserting")
        print(e)



# Hier beginnt die Spezifizierung des DAGs

"""
default_args = {
    'owner': 'BD-BI',
    'depends_on_past' : False,
    'start_date': datetime(2019, 12, 11),
    'email': ['alexander.dudko@baumarktdirekt.de'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '* * * * *',
}
"""

dag = DAG(
        'Load_ITM_Vorsysteme',
        start_date=datetime.datetime.now() - datetime.timedelta(days=1))

"""
dag = DAG(
        'Load_ITM_Vorsysteme',
        #'depends_on_past':False,
        start_date=datetime.datetime.now() - datetime.timedelta(days=1),
        #email=['alexander.dudko@baumarktdirekt.de'],
        #email_on_failure=True,
        #email_on_retry=True,
        #retries=1,
        #retry_delay=timedelta(minutes=1),
        schedule_interval='* * * * *',
        #default_args=default_args,
        #start_date=datetime.datetime.now() - datetime.timedelta(days=1)
        )
"""



conn_task = PythonOperator(
    task_id="conn_Oracle",
    python_callable=conn_oracle,
    dag=dag
)

cursor_task = PythonOperator(
        task_id="create_cursor",
        python_callable=create_cursor,
        dag=dag
        )

delete_ODS_KERN_TMP_MMDB_STYLE = PythonOperator(
        task_id="delete_ODS_KERN_TMP_MMDB_STYLE",
        python_callable=delete_ODS_KERN_TMP_MMDB_STYLE,
        dag=dag
        )


read_MMDB_csv_write_db = PythonOperator(
        task_id="read_MMDB_csv_write_db",
        python_callable=read_MMDB_csv_write_db,
        dag=dag
        )

conn_task >> cursor_task
cursor_task >> delete_ODS_KERN_TMP_MMDB_STYLE
delete_ODS_KERN_TMP_MMDB_STYLE >> read_MMDB_csv_write_db

#conn_oracle()
#create_cursor()
#delete_ODS_KERN_TMP_MMDB_STYLE()
#read_MMDB_csv()
#test_select()
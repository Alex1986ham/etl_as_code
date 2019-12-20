import datetime
from datetime import timedelta
import logging
import cx_Oracle
import sys
import csv
import xlrd 
import pandas as pd
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from openpyxl import load_workbook
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
#import airflow
#from airflow import DAG
#from airflow.operators.python_operator import PythonOperator


#conn = cx_Oracle.connect('STG01/oaoadmin@BDOPIS1/DWH1') # Keller
#cur = conn.cursor()
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


def delete_EMS_SCM_ABWERTUNGEN_AUFKAUF():
    try:
        cur.execute("DELETE FROM ODS.EMS_SCM_ABWERTUNGEN_AUFKAUF")
        conn.commit()
        print("delete successful")
    except cx_Oracle.Error as e:
        print("Error deleting")
        print(e)

def load_xlsx():
    #df = pd.read_excel('I:/obiatotto/_Übergreifende Themen und Projekte/OPIS/Schnittstelle/Schnittstellen_Templates_endgueltig/VS_FELIS/Abwertung.xlsx', 'Abwertung', skiprows=[1])
    #file = pd.ExcelFile('I:/obiatotto/_Übergreifende Themen und Projekte/OPIS/Schnittstelle/Schnittstellen_Templates_endgueltig/VS_FELIS/Abwertung.xlsx')
    with open ('I:/obiatotto/_Übergreifende Themen und Projekte/OPIS/Schnittstelle/Schnittstellen_Templates_endgueltig/VS_FELIS/Abwertung.xlsx') as file:
    #df = pd.DataFrame(file)
        data = file.parse(0)
    #next(data)
        for row in data:
          print(row)
    #file.sheet_names
    #df1 = file.parse(0)
    #print(df1)
    #print(df)
    #df2 = file._reader
    #cur.execute(
    #"INSERT INTO ODS.EMS_SCM_ABWERTUNGEN_AUFKAUF4 (AUFKAEUFER_NAME, LIEFERANTENNAME, LIEFERANT_CODE, CEE_LIEFERANTENNUMMER, KATEGORIE, ABWERTUNGSSATZ, GUELTIG_VON, GUELTIG_BIS) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)", data)
    #conn.commit()
    #for row in df1:
    #   print(row)
        #cur.execute(
        #"INSERT INTO ODS.EMS_SCM_ABWERTUNGEN_AUFKAUF4 (AUFKAEUFER_NAME, LIEFERANTENNAME, LIEFERANT_CODE, CEE_LIEFERANTENNUMMER, KATEGORIE, ABWERTUNGSSATZ, GUELTIG_VON, GUELTIG_BIS) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)", row)
        #conn.commit()


def load_excel():
    df = pd.read_excel('I:/obiatotto/_Übergreifende Themen und Projekte/OPIS/Schnittstelle/Schnittstellen_Templates_endgueltig/VS_FELIS/Abwertung.xlsx', 'Abwertung', skiprows=[0])
    #print(df.head())
    df.head()
    for row in df.head(5):
        print(row)
        #cur.execute(
        #"INSERT INTO ODS.EMS_SCM_ABWERTUNGEN_AUFKAUF4 (AUFKAEUFER_NAME, LIEFERANTENNAME, LIEFERANT_CODE, CEE_LIEFERANTENNUMMER, KATEGORIE, ABWERTUNGSSATZ, GUELTIG_VON, GUELTIG_BIS) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)", row)
        #conn.commit()


def read_MMDB_csv_write_db():
    file = 'I:/obiatotto/_Übergreifende Themen und Projekte/OPIS/Schnittstelle/Schnittstellen_Templates_endgueltig/VS_FELIS/Abwertung.xlsx'
    df = pd.DataFrame(data, file)
    included_cols = [0, 1, 2, 3, 4, 5, 6, 7]
    print(df)
    #next(df)
    #for row in df:
        #content = list(row[i] for i in included_cols)
        #print(row)
        #cur.execute(
        #"INSERT INTO ODS.EMS_SCM_ABWERTUNGEN_AUFKAUF2 (AUFKAEUFER_NAME, LIEFERANTENNAME, LIEFERANT_CODE, CEE_LIEFERANTENNUMMER, KATEGORIE, ABWERTUNGSSATZ, GUELTIG_VON, GUELTIG_BIS) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)", content)
        #conn.commit()
    #print(xl.sheet_names)
    #df1 = xl.parse('Abwertung')
    #for row in df1:
    #    print(df1)
        #cur.execute(
        #"INSERT INTO ODS.EMS_SCM_ABWERTUNGEN_AUFKAUF (AUFKAEUFER_NAME, LIEFERANTENNAME, LIEFERANT_CODE, CEE_LIEFERANTENNUMMER, KATEGORIE, ABWERTUNGSSATZ, GUELTIG_VON, GUELTIG_BIS) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)", row)
        #conn.commit()
    print("success inserting")
 
#--------------------------------------------------------------------------------------------------------------------------------------

def create_spark_session():
    """
    Cretes a Spark session
    """
    spark = SparkSession \
         .builder \
        .appName("Our first Python Spark SQL example") \
        .getOrCreate()
    print("Spark Session created")
    return spark

    
input_data = 'I:/obiatotto/_Übergreifende Themen und Projekte/OPIS/Schnittstelle/Schnittstellen_Templates_endgueltig/VS_FELIS/Abwertung.xlsx'
#spark = SparkSession \
#    .builder \
#    .appName("Our first Python Spark SQL example") \
#    .getOrCreate()

#df = spark.read.load(input_data)


#def process_data():
#    df = spark.read.ExcelFile(input_data)

#def main():
#    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    #output_data = "s3://dd-handel-gmbh.de/udacity_data_lake_project"

    #process_song_data(spark, input_data, output_data)
    #process_log_data(spark, input_data, output_data)


file_temp = "C:/Users/aldudko/Desktop/Temp/Abwertung.xlsx"

"""
def readExcel(file: String): DataFrame = spark.read. \
  format("com.crealytics.spark.excel").
  option("location", file).
  option("useHeader", "true").
  option("treatEmptyValuesAsNulls", "true").
  option("inferSchema", "true").
  option("addColorColumns", "False").
  load()
"""

df = pd.read_excel(file_temp)
sparkDF = sqlContext.createDataFrame(df)



#if __name__ == "__main__":
#    main()

#conn_oracle()
#create_cursor()
#delete_EMS_SCM_ABWERTUNGEN_AUFKAUF()

#read_MMDB_csv_write_db()
#openpyxl()
#load_xlsx()
#load_excel()
#create_spark_session()
#process_data()
#readExcel()
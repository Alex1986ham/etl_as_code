# coding=utf8
import cx_Oracle
#from sql_query import *
import sys
import csv
import os
import time, threading, schedule
import subprocess
subprocess.call(r'net use H: \\bdcifs\abteilung$', shell=True)

# Create connection to Oracle Database
try:
    conn = cx_Oracle.connect('DBTeam/DDfO10g@bmd-ora01/BMDDWH1')
    # Keller: conn = cx_Oracle.connect('DWH01/DWH01@BDOPIS1/DWH1')
    print("1. Connected to Oracle")
except cx_Oracle.Error as e:
    print("Error")
    print(e)

# Craete Cursor
try:
    cur = conn.cursor()
    print("2. Cursor created")
except cx_Oracle.Error as e:
    print("Error")
    print(e)

import mysql.connector                     #this module is for connection esablishment
import requests                            # this is to fetch the requests
from collections import Counter            # to count the no of time s a particular link occurs
from pathlib import Path                   # to check the path
import pandas as pd                        # to handel dataframes to import into excel files
import openpyxl                            # to handel and creation of excel files

# Connection establishment
try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="icm_db"
    )
    print("Connection established successfully!")
except mysql.connector.Error as err:
    print(f"Error: {err}")
    
cursor=conn.cursor()
    
query="update attachments set notes='mohan' where id=11"
cursor.execute(query)
conn.commit()

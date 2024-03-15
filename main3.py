import mysql.connector
import requests
import os
from collections import Counter

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

cursor = conn.cursor()

query = "SELECT * FROM attachments"
cursor.execute(query)
link = []
path = []
result = cursor.fetchall()

column_index = cursor.column_names.index('attachment_filename')
attachments_type=cursor.column_names.index('approval_type')

# query_opt = "select attachment_filename from attachments where attachment_filename like 'http:%' or attachment_filename like 'https:%'"
# cursor.execute(query_opt)
# res_opt=cursor.fetchall()

# print(res_opt)
# print(type(res_opt))

for row in result:
    if row[column_index].startswith("https://") or row[column_index].startswith("http://"):
        
        link.append(row[column_index])
    else:
 
        path.append(row[column_index])



uni = []

for i in range(len(link)):
    count_bypart = 0
    count_partseries = 0
    
    for row in result:
        if row[column_index] == link[i]:

            if row[attachments_type] == 'By part':
                count_bypart += 1
            elif row[attachments_type] == 'partseries':
                count_partseries += 1
    stng=f"{link[i]} has {count_bypart}  Byparts and  {count_partseries} partseries"
    uni.append(stng)

link_count = Counter(link)
for j, count in link_count.items():
    print(f"Link: {j}, Count: {count}")

for val in set(uni):
    print(val)
print(len(set(uni)))










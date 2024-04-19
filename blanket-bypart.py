from pathlib import Path
import mysql.connector
import requests
from collections import Counter
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

cursor = conn.cursor()

# Select the attachment table
query = "SELECT * FROM attachments"
cursor.execute(query)

# Store links and paths from the attachment table
link = []
path = []
result = cursor.fetchall()
column_index = cursor.column_names.index('attachment_filename')
attachments_type = cursor.column_names.index('approval_type')
comp_id_attachments=cursor.column_names.index("component_id")


for row in result:
    if row[column_index].startswith("https://") or row[column_index].startswith("http://"):
        link.append(row[column_index])
    else:
        path.append(row[column_index])


query2="select * from attachments where approval_type='By part'"
cursor.execute(query2)
result_out=cursor.fetchall()
result_id=cursor.column_names.index("component_id")
result_attachmentname=cursor.column_names.index("attachment_filename")
result_approvaltype=cursor.column_names.index("approval_type")            
attachment_dict = {}


query3= 'select * from attachments'
cursor.execute(query3)
appro_type=cursor.column_names.index("approval_type")
attc_type_qu3=cursor.column_names.index("attachment_filename")
comp_id_q3=cursor.column_names.index("component_id")
res_qu3=cursor.fetchall()
attachment_dict = {}
c_ids = []
strg = []
for row in result_out:
    c_ids = []
    for row1 in res_qu3:
        if (row1[attc_type_qu3] == row[result_attachmentname] and row1[appro_type] == "blanket"):
            #print(row1[comp_id_q3])
            c_ids.append(row1[comp_id_q3])
    attachment_dict[row[result_attachmentname]] = c_ids  

for attachment, component_ids in attachment_dict.items():
    if(len(component_ids)>1):
        print(f"Attachment: {attachment}")
        print("Component IDs:")
        for component_id in component_ids:
            print(component_id)

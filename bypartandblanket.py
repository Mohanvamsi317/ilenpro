from pathlib import Path
import mysql.connector
import requests
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
attachments_type = cursor.column_names.index('approval_type')
comp_id_attachments = cursor.column_names.index("component_id")

for row in result:
    if row[column_index].startswith("https://") or row[column_index].startswith("http://"):
        link.append(row[column_index])
    else:
        path.append(row[column_index])

# Query to select attachments with approval type 'By part'
query_by_part = "SELECT * FROM attachments WHERE approval_type = 'By part'"
cursor.execute(query_by_part)
result_by_part = cursor.fetchall()
result_id = cursor.column_names.index("component_id")
result_attachmentname = cursor.column_names.index("attachment_filename")
# Query to select attachments with approval type 'partseries'
query_part_series = "SELECT * FROM attachments WHERE approval_type = 'blanket'"
cursor.execute(query_part_series)
result_part_series = cursor.fetchall()
comp_id_q3 = cursor.column_names.index("component_id")
attach_filename_q3 = cursor.column_names.index("attachment_filename")
attachment_dict = {}
for row1 in result_by_part:
    component_ids = []
    for row in result_part_series:
        if row[attach_filename_q3] == row1[result_attachmentname]:
            component_ids.append(row[comp_id_q3])           
    attachment_dict[row1[result_attachmentname]] = component_ids
for attachment, component_ids in attachment_dict.items():
    if(len(component_ids)>0):
        print(f"Attachment: {attachment}")
        print("Component IDs:")
        for component_id in component_ids:
            print(component_id)

# program for the doc is by part and mapped to more than two components
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

#verify weather link or path

for row in result:
    if row[column_index].startswith("https://") or row[column_index].startswith("http://"):
        link.append(row[column_index])
    else:
        path.append(row[column_index])


query_on_duplicate="select * from attachments"
cursor.execute(query_on_duplicate)
results_of_duplicates=cursor.fetchall()
results_of_duplicates_id=cursor.column_names.index("component_id")
results_of_duplicates_attachment_file=cursor.column_names.index("attachment_filename")
results_of_duplicates_approval=cursor.column_names.index("approval_type")
#print(results_of_duplicates)            
attachment_dict = {}

for link_item in link:
    for row in results_of_duplicates:
        if row[results_of_duplicates_attachment_file] == link_item and row[results_of_duplicates_approval] == "By part":
            query_component_id = "select * from attachments where attachment_filename = %s and approval_type='By part'"
            attachment_filename = row[results_of_duplicates_attachment_file]
            cursor.execute(query_component_id, (attachment_filename,))
            result_component_id = cursor.fetchall()
            id=row[comp_id_attachments]
            component_ids =[id]            
            for res in result_component_id:
                if res[comp_id_attachments] != row[results_of_duplicates_id]:
                    component_ids.append(res[comp_id_attachments])
            attachment_dict[attachment_filename] = component_ids
for attachment, component_ids in attachment_dict.items():
    print(f"Attachment: {attachment}")
    print("Component IDs:")
    for component_id in component_ids:
       print(component_id)





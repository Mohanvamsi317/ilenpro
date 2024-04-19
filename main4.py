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










# # Assuming manufacture_results, link_of_duplicate, and other variables are defined elsewhere
# download_all_files()





# Function to download files
# def download_file(file_url, local_directory):
#     try:
#         # Get request
#         response = requests.get(file_url)
#         #split the query parameter if exists else it takes the link only and append that to the path parameter
#         filename = local_directory / Path(file_url.split('?')[0]).name.strip()
        
#         # If the file exists
#         if filename.exists():
#             print("File already exists:")
#             return True

#         # If not present, create it
#         with open(filename, 'wb') as local_file:
#             local_file.write(response.content)
        
#         print(f"File downloaded and saved as {filename}")
#         return True
#     except requests.exceptions.RequestException as e:
#         print(f"Error downloading file: {e}")
#         return False
    
# # Query to execute the manufacturers table
# query1 = "SELECT * FROM manufacturers"
# cursor.execute(query1)
# manufacture_results = cursor.fetchall()
# #column_names.index is a method to access the index of the particular colum as the list item cant be accessed by its 
# manufacture_name_manufacture = cursor.column_names.index('name')
# manufucture_index = cursor.column_names.index('normalized_name')
# manufacturer_id_index = cursor.column_names.index('id')

# # Query to execute the master_components table



# #function to download the files
# def download_all_file():
#     #user input to enter the path
#     pat = input("Enter the path you want to insert the folders: ")
#     for row in manufacture_results:
#         manufacturer_id = row[manufacturer_id_index]
#         directory_name = row[manufucture_index]
#         manu_name=row[manufacture_name_manufacture]
#         #convert the entered string module to the path variable
#         local_directory = Path(pat) / directory_name
#         try:
#             # If the directory exists, enter it
#             if local_directory.exists():
#                 print("already exists:", local_directory)
#             # If not present, create it
#             else:
#                 local_directory.mkdir()
#                 print("Directory created:", local_directory)
            
#             # Compilance directory
#             directory_name_comp = 'compilance'
#             local_directory_compilance = local_directory / directory_name_comp
#             # if the directory exists, enter it
#             if local_directory_compilance.exists():
#                 print("alredy present compilance directory")
#             #else create it
#             else:
#                     local_directory_compilance.mkdir()
#                     print("Compilance directory created:", local_directory_compilance)
#             query2 = "SELECT * FROM master_components where manufacturer_name = %s"
#             cursor.execute(query2,(manu_name,))
#             component_results = cursor.fetchall()
#             manufacture_part_in_master =cursor.column_names.index('manufacturer_partnumber')
#             component_id_component = cursor.column_names.index('id')
#             manufacture_name_component =cursor.column_names.index('manufacturer_name')

#             for component_row in component_results:
#                         if component_row[manufacture_name_component] == row[manufacture_name_manufacture]:
#                             component_id = component_row[component_id_component] 
#                             directory_name1 = component_row[manufacture_part_in_master] 
#                             local_directory2 = local_directory_compilance / directory_name1

#                             if local_directory2.exists():
#                                 print("Entered component part folder already existing")
#                             else:    
#                                 local_directory2.mkdir(parents=True, exist_ok=True)
#                                 print(f"Subdirectory created: {local_directory2}")

#                             # query3 = "select * from attachments where manufacturer_id = %s AND component_id = %s and attachment_filename not in (%s) "
#                             # cursor.execute(query3, (manufacturer_id, component_id,link_of_duplicate))
#                             # attachment_results = cursor.fetchall()
                            
#                         # Generate placeholders for attachment filenames
                        
#                             try:
#                                 if link_of_duplicate:
#                                     attachment_placeholders = ', '.join(['%s'] * len(link_of_duplicate))
#                                     query3 = f"SELECT * FROM attachments WHERE manufacturer_id = %s AND component_id = %s AND attachment_filename NOT IN ({attachment_placeholders})"
#                                     query_params = [manufacturer_id, component_id] + link_of_duplicate
#                                     print(query_params)
#                                     cursor.execute(query3, query_params)
#                                     attachment_results = cursor.fetchall()
#                             except Exception as e:
#                                 print("Error executing query:", e)
#             print(attachment_results)
#             for attachment_row in attachment_results:
#                         file_url = attachment_row[column_index]
#                         file_type = attachment_row[attachments_type]

#                         if file_type == "partseries":
#                             print("It's a part series file")
#                             directory_name_partseries='partseries'
#                             local_directory_partseries=local_directory_compilance / directory_name_partseries
#                             if local_directory_partseries.exists():
#                                 print("the directory already exists:",local_directory_partseries)
#                             else:
#                                 local_directory_partseries.mkdir()
                                
#                                 print("partseries directory created:", local_directory_partseries)
#                             download_file(file_url, local_directory_partseries)
#                         elif file_type == "blanket":
#                             print("It's a blanket file")
#                             directory_name_blanket = 'blanket'
#                             local_directory_blanket = local_directory_compilance / directory_name_blanket

#                             if local_directory_blanket.exists():
                                
#                                 print("This directory already exists:", local_directory_blanket)
#                             else:
#                                 local_directory_blanket.mkdir()
#                                 print("Blanket directory created:", local_directory_blanket)

#                             download_file(file_url, local_directory_blanket)
#                         else:
#                             print("its a bypart file:")
#                             download_file(file_url, local_directory2)
#             for i in link_of_duplicate:
#                         dir_name_multiple="mutlple"
#                         local_directory_mutliple=Path(pat)/dir_name_multiple
#                         if local_directory_mutliple.exists():
#                             print("directory for multiple files was created :",local_directory_mutliple)
#                         else:
#                             local_directory_mutliple.mkdir()
#                         download_file(i,local_directory_mutliple)
                        

#         except Exception as e:
#             print(f"Error: {e}")
            


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

query = "SELECT * FROM attachments limit 5"
cursor.execute(query)
#to store links
link = []
path = []
#to fetch all the records in the data base
result = cursor.fetchall()

column_index = cursor.column_names.index('attachment_filename')
attachments_type=cursor.column_names.index('approval_type')

for row in result:
    if row[column_index].startswith("https://") or row[column_index].startswith("http://"):
        
        link.append(row[column_index])
    else:
        path.append(row[column_index])
        
        
        
        
# to count the occurance of the each link and along with each link how many times it is part series and how many times it was by part and total no of links etc      

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
        
        
 #this is a function used to download the files which are in the https and downladeble formats       
        
def download_file(file_url, local_directory):
    try:
        response = requests.get(file_url)
        filename = os.path.join(local_directory, os.path.basename(file_url.split('?')[0]).strip())
        
        
        if os.path.exists(filename):
            print(f"File already exists: {filename}")
            return True

        
        with open(filename, 'wb') as local_file:
            local_file.write(response.content)
        
        print(f"File downloaded and saved as {filename}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return False
    
# this is the query to execute the attachments table arguments
query1 = "SELECT * FROM manufacturers"
cursor.execute(query1)
manufacture_results = cursor.fetchall()
manufacture_name_manufacture=cursor.column_names.index('name')
manufucture_index = cursor.column_names.index('normalized_name')
manufacturer_id_index = cursor.column_names.index('id')
print(manufacturer_id_index)
print(manufucture_index)



#this is the query used to execute the master_components table
query2 = "SELECT * FROM master_components"
cursor.execute(query2)
component_results = cursor.fetchall()
manufacture_part_in_master = cursor.column_names.index('manufacturer_partnumber')
component_id_component = cursor.column_names.index('id')
manufacture_name_component=cursor.column_names.index('manufacturer_name')
user_path=input("enter the location:")

for row in manufacture_results:
    manufacturer_id = row[manufacturer_id_index]
    directory_name = row[manufucture_index]
    local_directory = os.path.join(user_path, directory_name)
    try:
        os.makedirs(local_directory)
        print(f"Directory created: {local_directory}")
        
        
        try:
            directory_name_comp='compilance'
            local_directory_compilance=os.path.join(local_directory,directory_name_comp)
            os.makedirs(local_directory_compilance)
            

            for component_row in component_results:
                if component_row[manufacture_name_component] == row[manufacture_name_manufacture]:
                    component_id = component_row[component_id_component] #7
                    directory_name1 = component_row[manufacture_part_in_master] #G9EJ-1-E-UVDDC12
                    local_directory2 = os.path.join(local_directory_compilance, directory_name1)
                
                    try:
                        os.makedirs(local_directory2)
                        print(f"Subdirectory created: {local_directory2}")
                        query3 = "SELECT * FROM attachments WHERE manufacturer_id = %s AND component_id = %s"
                        cursor.execute(query3, (manufacturer_id, component_id))
                        attachment_results = cursor.fetchall()

                        for attachment_row in attachment_results:
                            if(attachment_row[attachments_type]=="partseries"):
                                file_url=attachment_row[column_index]
                                download_file(file_url,local_directory_compilance)
                            
                            file_url = attachment_row[column_index]
                            #print(file_url)
                            if file_url.startswith("https://") or file_url.startswith("http://"):
                                download_file(file_url, local_directory2)
                            else:
                                print(f"Invalid URL: {file_url}")

                    except FileExistsError:
                        print(f"Subdirectory already exists: {local_directory2}")
        except FileExistsError:
            print(f"compilence directory already exists: {local_directory_compilance}")

    except FileExistsError:
        print(f"Directory already exists: {local_directory}")

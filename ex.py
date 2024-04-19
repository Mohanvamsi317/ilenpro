import mysql.connector
import requests
from collections import Counter
from pathlib import Path

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

# Selecting data from the attachments table
query = "SELECT * FROM attachments LIMIT 5"
cursor.execute(query)

# Lists to store links and paths
link = []
path = []

# Fetching all records in the database
result = cursor.fetchall()

# Getting the column indices
column_names = cursor.column_names
column_index = column_names.index('attachment_filename')
attachments_type = column_names.index('approval_type')

# Loop to check whether it's a link or a path
for row in result:
    if row[column_index].startswith("https://") or row[column_index].startswith("http://"):
        link.append(row[column_index])
    else:
        path.append(row[column_index])

# Counting the occurrences of each link
link_count = Counter(link)
for j, count in link_count.items():
    print(f"Link: {j}, Count: {count}")

# Creating a set of strings
uni = [f"{l} has {link_count[l]} Byparts and {link_count[l]} partseries" for l in set(link)]
for val in uni:
    print(val)

# Function to download files
def download_file(file_url, local_directory):
    try:
        # Get request
        response = requests.get(file_url)
        
        filename = local_directory / Path(file_url.split('?')[0]).name.strip()
        
        # If the file exists
        if filename.exists():
            print("File already exists:")
            return True

        # If not present, create it
        with open(filename, 'wb') as local_file:
            local_file.write(response.content)
        
        print(f"File downloaded and saved as {filename}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return False
    
# Query to execute the manufacturers table
query1 = "SELECT * FROM manufacturers"
cursor.execute(query1)
manufacture_results = cursor.fetchall()
manufacture_name_manufacture = cursor.column_names.index('name')
manufucture_index = cursor.column_names.index('normalized_name')
manufacturer_id_index = cursor.column_names.index('id')

# Query to execute the master_components table
query2 = "SELECT * FROM master_components"
cursor.execute(query2)
component_results = cursor.fetchall()
manufacture_part_in_master =cursor.column_names.index('manufacturer_partnumber')
component_id_component = cursor.column_names.index('id')
manufacture_name_component =cursor.column_names.index('manufacturer_name')

# Input path from the user
pat = input("Enter the path you want to insert the folders: ")

for row in manufacture_results:
    manufacturer_id = row[manufacturer_id_index]
    directory_name = row[manufucture_index]
    local_directory = Path(pat) / directory_name
    try:
        # If the directory exists, enter it
        if local_directory.exists():
            print("already exists:", local_directory)
        # If not present, create it
        else:
            local_directory.mkdir()
            print("Directory created:", local_directory)
        
        # Compilance directory
        directory_name_comp = 'compilance'
        local_directory_compilance = local_directory / directory_name_comp

        if local_directory_compilance.exists():
            print("alredy present compilance directory")
        else:
            local_directory_compilance.mkdir(parents=True, exist_ok=True)
            print("Compilance directory created:", local_directory_compilance)

        for component_row in component_results:
            if component_row[manufacture_name_component] == row[manufacture_name_manufacture]:
                component_id = component_row[component_id_component] 
                directory_name1 = component_row[manufacture_part_in_master] 
                local_directory2 = local_directory_compilance / directory_name1

                if local_directory2.exists():
                    print("Entered component part folder already existing")
                else:    
                    local_directory2.mkdir(parents=True, exist_ok=True)
                    print(f"Subdirectory created: {local_directory2}")

                query3 = "select * from attachments where manufacturer_id = %s AND component_id = %s"
                cursor.execute(query3, (manufacturer_id, component_id))
                attachment_results = cursor.fetchall()

                for attachment_row in attachment_results:
                    file_url = attachment_row[column_index]
                    file_type = attachment_row[attachments_type]

                    if file_type == "partseries":
                        print("It's a part series file")
                        download_file(file_url, local_directory_compilance)
                    elif file_type == "blanket":
                        print("It's a blanket file")
                        directory_name_blanket = 'blanket'
                        local_directory_blanket = local_directory_compilance / directory_name_blanket

                        if local_directory_blanket.exists():
                            print("This directory already exists:", local_directory_blanket)
                        else:
                            local_directory_blanket.mkdir(parents=True, exist_ok=True)
                            print("Blanket directory created:", local_directory_blanket)

                        download_file(file_url, local_directory_blanket)
                    else:
                        download_file(file_url, local_directory2)

    except Exception as e:
        print(f"Error: {e}")

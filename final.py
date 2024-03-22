import mysql.connector
import requests
from collections import Counter
from pathlib import Path
import pandas as pd
import openpyxl

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
query = "SELECT * FROM attachments"
cursor.execute(query)

# Lists to store links and paths
link = []
path = []


# Fetching all records in the database
result = cursor.fetchall()

# Getting the column indices
column_index = cursor.column_names.index('attachment_filename')
attachments_type = cursor.column_names.index('approval_type')
comp_id_attachments=cursor.column_names.index("component_id")

# Loop to check whether it's a link or a path

for row in result:
    if row[column_index].startswith("https://") or row[column_index].startswith("http://"):
        link.append(row[column_index])
    else:
        path.append(row[column_index])

    # Counting the occurrences of each link
def count_of_linktypes():

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
        
    # used to find the no of links in the table using the set method

    for val in set(uni):
        print(val)
    #print(len(set(uni)))
    df_link_count = pd.DataFrame(link_count.items(), columns=['Link', 'Count'])
    df_uni = pd.DataFrame(uni, columns=['Details'])
    
    # Save dataframes to Excel
    with pd.ExcelWriter('count_of_linktypes.xlsx') as writer:
        df_link_count.to_excel(writer, sheet_name='Link Counts', index=False)
        df_uni.to_excel(writer, sheet_name='Link Details', index=False)

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



def download_all_file():
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
            
            
            
def duplicate_byparts():
    query_on_duplicate = "SELECT * FROM attachments"
    cursor.execute(query_on_duplicate)
    results_of_duplicates = cursor.fetchall()
    
    results_of_duplicates_attachment_file = cursor.column_names.index("attachment_filename")
    results_of_duplicates_approval = cursor.column_names.index("approval_type")
    
    attachment_dict = {}

    for link_item in link:
        for row in results_of_duplicates:
            if row[results_of_duplicates_attachment_file] == link_item and row[results_of_duplicates_approval] == "By part":
                query_component_id = "SELECT * FROM attachments WHERE attachment_filename = %s AND approval_type = 'By part'"
                attachment_filename = row[results_of_duplicates_attachment_file]
                cursor.execute(query_component_id, (attachment_filename,))
                result_component_id = cursor.fetchall()
                results_of_duplicates_id = cursor.column_names.index("component_id")
                component_ids =[row[results_of_duplicates_id]]
                for res in result_component_id:
                    if res[comp_id_attachments] != row[results_of_duplicates_id]:
                        component_ids.append(res[comp_id_attachments])
                if len(component_ids) > 1:
                    attachment_dict[attachment_filename] = component_ids
    
    if attachment_dict:
        df_attachment_dict = pd.DataFrame(attachment_dict.items(), columns=['Attachment', 'Component IDs'])
        # Save dataframe to Excel
        with pd.ExcelWriter('duplicate_byparts.xlsx') as writer:
            df_attachment_dict.to_excel(writer, index=False)


def bypartandblanket():
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
                component_ids.append(row1[result_id])        
        attachment_dict[row1[result_attachmentname]] = component_ids
  
    # Filter attachments with component IDs greater than 0
    attachment_dict_filtered = {attachment: component_ids 
                                for attachment, component_ids in attachment_dict.items() 
                                if set(component_ids) and len(component_ids) > 0  }
    
    df_attachment_dict = pd.DataFrame(attachment_dict_filtered.items(), columns=['Attachment', 'Component IDs'])
    #print(df_attachment_dict)
   # print(df_attachment_dict.empty)

    if  df_attachment_dict.empty:
         print("No attachments with Component IDs greater than 0.")
       
    else:
        print("Attachments with Component IDs:")
        print(df_attachment_dict)
        with pd.ExcelWriter('bypartblanket.xlsx') as writer:
            df_attachment_dict.to_excel(writer, index=False)
       



def partseriesbypart():
    query_by_part = "SELECT * FROM attachments WHERE approval_type = 'By part'"
    cursor.execute(query_by_part)
    result_by_part = cursor.fetchall()
    result_attachmentname = cursor.column_names.index("attachment_filename")
    result_component_id=cursor.column_names.index("component_id")
    
    # Query to select attachments with approval type 'partseries'
    query_part_series = "SELECT * FROM attachments WHERE approval_type = 'partseries'"
    cursor.execute(query_part_series)
    result_part_series = cursor.fetchall()
    comp_id_q3 = cursor.column_names.index("component_id")
    attach_filename_q3 = cursor.column_names.index("attachment_filename")
    
    attachment_dict = {}
    for row1 in result_by_part:
        component_ids = []
        for row in result_part_series:
            if row[attach_filename_q3] == row1[result_attachmentname]:
                component_ids.append(row1[result_component_id])
                component_ids.append(row[comp_id_q3])           
        attachment_dict[row1[result_attachmentname]] = component_ids

    # Filter attachments with component IDs greater than 0
    attachment_dict_filtered = {attachment: component_ids 
                                for attachment, component_ids in attachment_dict.items() 
                                if len(component_ids) > 0}

    if attachment_dict_filtered:
        print("Attachments with Component IDs:")
        for attachment, component_ids in attachment_dict_filtered.items():
            print(f"Attachment: {attachment}")
            print("Component IDs:")
            for component_id in component_ids:
                print(component_id)

        df_attachment_dict = pd.DataFrame(attachment_dict_filtered.items(), columns=['Attachment', 'Component IDs'])
        with pd.ExcelWriter('partseriesbypart.xlsx') as writer:
            df_attachment_dict.to_excel(writer, index=False)
    else:
        print("No attachments with Component IDs greater than 0.")

        
def selection(key):
    if key==1:
        count_of_linktypes()
    elif key==2:
        partseriesbypart()
    elif key==3:
        bypartandblanket()
    elif key==4:
        print("duplicate byparts")
        duplicate_byparts()
    elif key==5:
        download_all_file()
    else:
        print("invalid input")
        
print("enter the valid input:")

print(" menu :")
print("1: count the number of links and the type of links")
print("2:to find out the links weather it was both partseries and bypart")
print("3: to find out the links weather it was both bypart and blanket")
print("4: to find out the links weather it was both are byparts")
print("5: to download the file")

inp=int(input("enter your choice:"))
selection(inp)



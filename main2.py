import mysql.connector
import requests
import os
from collections import Counter

#connection establishment
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
#to select the attachment table
query = "SELECT * FROM attachments limit 5"
cursor.execute(query)

#to store links i attachment table
link = []
path = []
#to fetch all the records in the data base
result = cursor.fetchall()
#to acess the attachcment _filename in attahment table
column_index = cursor.column_names.index('attachment_filename')
#to access the approval type iy may be bypart or partserries
attachments_type=cursor.column_names.index('approval_type')
#for loop to check weather it was a link or a path
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
    
# used to find the no of links in the table using the set method

for val in set(uni):
    print(val)
print(len(set(uni)))
        
        
 #this is a function used to download the files which are in the https and downladeble formats       
        
def download_file(file_url, local_directory):
    try:
        #to get request 
        response = requests.get(file_url)
        
        filename = os.path.join(local_directory, os.path.basename(file_url.split('?')[0]).strip())
        
        #if the file exits
        if os.path.exists(filename):
            print("File already exists:")
            return True

        # if not present it will create
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
#fetch the results in the manufacture table
manufacture_results = cursor.fetchall()
#fetc the index in the manufacture table in which column is name
manufacture_name_manufacture=cursor.column_names.index('name')
#fetch the index of the normalized_name in the manufacturer table
manufucture_index = cursor.column_names.index('normalized_name')
#fetch the index of id inthe manufacturer table
manufacturer_id_index = cursor.column_names.index('id')
print(manufacturer_id_index)
print(manufucture_index)



#this is the query used to execute the master_components table 
query2 = "SELECT * FROM master_components"
cursor.execute(query2)
#fetch the results in the master_component table
component_results = cursor.fetchall()
#fetch the index of the  manufacturer_partnumber in in the master_component table 
manufacture_part_in_master = cursor.column_names.index('manufacturer_partnumber')
#fetch the index of the id in the mater_component tbale
component_id_component = cursor.column_names.index('id')
# fetch the index of the manufacturer_name in the master_component table
manufacture_name_component=cursor.column_names.index('manufacturer_name')


#to enter the input from the user
pat=input("enter the path you want to insert the folders:")

for row in manufacture_results:
    manufacturer_id = row[manufacturer_id_index]
    directory_name = row[manufucture_index]
    #to serch the directory path
    local_directory = os.path.join(pat, directory_name)
    try:
        #if the directory exists it will enter into that directory
        if  os.path.exists(local_directory):
            os.chdir(local_directory)
            print("entered" f'{local_directory}')
        #if not present then it will vcreate the new directory
        else:
            os.makedirs(local_directory)
            print(f"Directory created: {local_directory}")
        
        
        try:
            
            directory_name_comp='compilance'
            local_directory_compilance=os.path.join(local_directory,directory_name_comp)
            
            #if the directory exists it will enter into that directory
            if  os.path.exists(local_directory_compilance):
                os.chdir(local_directory_compilance)
                print("entered existing compilance directory")
                
            #if not present then it will vcreate the new directory
            else:
                os.makedirs(local_directory_compilance)
            

            for component_row in component_results:
                if component_row[manufacture_name_component] == row[manufacture_name_manufacture]:
                    component_id = component_row[component_id_component] 
                    directory_name1 = component_row[manufacture_part_in_master] #G9EJ-1-E-UVDDC12
                    local_directory2 = os.path.join(local_directory_compilance, directory_name1)
                
                    try:
                        if  os.path.exists(directory_name1):
                            os.chdir(directory_name1)
                            print("entered component part folder already existing")
                        else:    
                            os.makedirs(local_directory2)
                            print(f"Subdirectory created: {local_directory2}")
                        query3 = "SELECT * FROM attachments WHERE manufacturer_id = %s AND component_id = %s"
                        cursor.execute(query3, (manufacturer_id, component_id))
                        attachment_results = cursor.fetchall()
                        print(f"for the manufacturer id {manufacturer_id} and the component id {component_id} there are{count(attachment_results)} records")

                        for attachment_row in attachment_results:
                            if(attachment_row[attachments_type]=="partseries"):
                                try:
                                    print("its a part series file")
                                    
                                    file_url=attachment_row[column_index]
                                    download_file(file_url,local_directory_compilance)
                                except Exception:
                                    print("uncaught exception")
                            elif(attachment_row[attachments_type]=="blanket"):
                                try:
                                    directory_name_blanket = "blanket"
                                    local_directory_blanket = os.path.join(local_directory_compilance, directory_name_blanket)
                                    print("ita a blanket file")
                                    
                                    if(os.path.exists(local_directory_blanket)):
                                        os.chdir(local_directory_blanket)
                                        print("this directory is a existing blanket directory")
                                    else:
                                        print("else")
                                        os.makedirs(local_directory_blanket)
                                        print("dir creates")
                                        file_url_blanket=attachment_row[column_index]
                                        download_file(file_url_blanket,local_directory_blanket)
                                except Exception:
                                    print("uncaught")
                            else:
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

    except Exception:
        print(f"Directory already exists: {local_directory}")
        

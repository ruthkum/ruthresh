#AGGREGATED TRANSACTION :

import pandas as pd
import json
import os

# This is to direct the path to get the data as states
path = "D:/PhonePe Project/pulse/data/aggregated/transaction/country/india/state"
Agg_state_list = os.listdir(path)

# This is to extract the data to create a dataframe
clm = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in Agg_state_list:
    p_i = os.path.join(path, i)

    Agg_yr = os.listdir(p_i)
    for j in Agg_yr:
        p_j = os.path.join(p_i, j)
        Agg_yr_list = os.listdir(p_j)
        for k in Agg_yr_list:
            p_k = os.path.join(p_j, k)
            with open(p_k, 'r') as Data:
                D = json.load(Data)
                for z in D['data']['transactionData']:
                    Name = z['name']
                    count = z['paymentInstruments'][0]['count']
                    amount = z['paymentInstruments'][0]['amount']
                    clm['Transaction_type'].append(Name)
                    clm['Transaction_count'].append(count)
                    clm['Transaction_amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))

# Successfully created a dataframe
Agg_Trans = pd.DataFrame(clm)
Agg_Trans

# AGGREGATED TRANSACTION :

import pandas as pd
import json
import os

# This is to direct the path to get the data as states
path = "D:/PhonePe Project/pulse/data/aggregated/transaction/country/india/state"
Agg_state_list = os.listdir(path)

# This is to extract the data to create a dataframe
clm = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in Agg_state_list:
    p_i = os.path.join(path, i)

    Agg_yr = os.listdir(p_i)
    for j in Agg_yr:
        p_j = os.path.join(p_i, j)
        Agg_yr_list = os.listdir(p_j)
        for k in Agg_yr_list:
            p_k = os.path.join(p_j, k)
            with open(p_k, 'r') as Data:
                D = json.load(Data)
                for z in D['data']['transactionData']:
                    Name = z['name']
                    count = z['paymentInstruments'][0]['count']
                    amount = z['paymentInstruments'][0]['amount']
                    clm['Transaction_type'].append(Name)
                    clm['Transaction_count'].append(count)
                    clm['Transaction_amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))

# Successfully created a dataframe
Agg_Trans = pd.DataFrame(clm)
Agg_Trans

# AGGREGATED USER  :

import pandas as pd
import json
import os

# This is to direct the path to get the data as states
path = "D:/PhonePe Project/pulse/data/aggregated/user/country/india/state"
Agg_state_list = os.listdir(path)

# This is to extract the data to create a dataframe
columns_2 = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'Count': [], 'Percentage': []}

for state in Agg_state_list:
    p_i = os.path.join(path, state)

    Agg_yr = os.listdir(p_i)
    for year in Agg_yr:
        p_j = os.path.join(p_i, year)
        Agg_yr_list = os.listdir(p_j)
        for file in Agg_yr_list:
            p_k = os.path.join(p_j, file)
            with open(p_k, 'r') as Data:
                try:
                    D = json.load(Data)

                    # Check if the required keys exist
                    for i in D["data"]["usersByDevice"]:
                        brand_name = i["brand"]
                        counts = i["count"]
                        percents = i["percentage"]
                        columns_2["Brands"].append(brand_name)
                        columns_2["Count"].append(counts)
                        columns_2["Percentage"].append(percents)
                        columns_2["State"].append(state)
                        columns_2["Year"].append(year)
                        columns_2["Quarter"].append(int(file.strip('.json')))
                except KeyError as e:
                    pass
                except Exception as e:
                    pass

# Successfully created a dataframe
Agg_Brands = pd.DataFrame(columns_2)
Agg_Brands

# MAP TRANSACTION :

import pandas as pd
import json
import os

# This is to direct the path to get the data as states
path = "D:/PhonePe Project/pulse/data/map/transaction/hover/country/india/state"
Agg_state_list = os.listdir(path)

# This is to extract the data to create a dataframe
columns_3 = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Count': [], 'Amount': []}

for state in Agg_state_list:
    p_i = os.path.join(path, state)

    Agg_yr = os.listdir(p_i)
    for year in Agg_yr:
        p_j = os.path.join(p_i, year)
        Agg_yr_list = os.listdir(p_j)
        for file in Agg_yr_list:
            p_k = os.path.join(p_j, file)
            with open(p_k, 'r') as Data:
                D = json.load(Data)
                data_key = 'hoverDataList'  # Adjust this key based on your actual data structure
                
                if 'data' in D and data_key in D['data']:
                    for i in D['data'][data_key]:
                        district = i.get("name", "Unknown District")
                        count = i["metric"][0].get("count", 0)
                        amount = i["metric"][0].get("amount", 0)
                        columns_3["District"].append(district)
                        columns_3["Count"].append(count)
                        columns_3["Amount"].append(amount)
                        columns_3['State'].append(state)
                        columns_3['Year'].append(year)
                        columns_3['Quarter'].append(int(file.strip('.json')))
                else:
                    print(f"Missing key '{data_key}' in {p_k}")

# Successfully created a dataframe
Agg_Trans = pd.DataFrame(columns_3)
Agg_Trans

# MAP USER :



import pandas as pd
import json
import os

# This is to direct the path to get the data as states
path = "D:/PhonePe Project/pulse/data/map/user/hover/country/india/state"
Agg_state_list = os.listdir(path)

# This is to extract the data to create a dataframe
columns_4 = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'RegisteredUser': [], 'AppOpens': []}

for state in Agg_state_list:
    p_i = os.path.join(path, state)

    Agg_yr = os.listdir(p_i)
    for year in Agg_yr:
        p_j = os.path.join(p_i, year)
        Agg_yr_list = os.listdir(p_j)
        for file in Agg_yr_list:
            p_k = os.path.join(p_j, file)
            with open(p_k, 'r') as Data:
                try:
                    D = json.load(Data)

                    for district, data in D["data"]["hoverData"].items():
                        registereduser = data["registeredUsers"]
                        appOpens = data['appOpens']

                        columns_4["District"].append(district)
                        columns_4["RegisteredUser"].append(registereduser)
                        columns_4["AppOpens"].append(appOpens)
                        columns_4['State'].append(state)
                        columns_4['Year'].append(year)
                        columns_4['Quarter'].append(int(file.strip('.json')))
                except Exception as e:
                    print(f"An error occurred while processing {p_k}: {e}")

# Successfully created a dataframe
Agg_Trans = pd.DataFrame(columns_4)
Agg_Trans

# TOP TRANSACTION :

import pandas as pd
import json
import os

# This is to direct the path to get the data as states
path = "D:/PhonePe Project/pulse/data/top/transaction/country/india/state"
Agg_state_list = os.listdir(path)

# This is to extract the data to create a dataframe
columns_5 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count': [], 'Transaction_amount': []}

for state in Agg_state_list:
    p_i = os.path.join(path, state)

    Agg_yr = os.listdir(p_i)
    for year in Agg_yr:
        p_j = os.path.join(p_i, year)
        Agg_yr_list = os.listdir(p_j)
        for file in Agg_yr_list:
            p_k = os.path.join(p_j, file)
            with open(p_k, 'r') as Data:
                try:
                    D = json.load(Data)

                    for pincode_data in D['data']['pincodes']:
                        name = pincode_data['entityName']
                        count = pincode_data['metric']['count']
                        amount = pincode_data['metric']['amount']

                        columns_5['Pincode'].append(name)
                        columns_5['Transaction_count'].append(count)
                        columns_5['Transaction_amount'].append(amount)
                        columns_5['State'].append(state)
                        columns_5['Year'].append(year)
                        columns_5['Quarter'].append(int(file.strip('.json')))
                except Exception as e:
                    print(f"An error occurred while processing {p_k}: {e}")

# Successfully created a dataframe
Agg_Trans = pd.DataFrame(columns_5)
Agg_Trans

# TOP USER :

import pandas as pd
import json
import os

# This is to direct the path to get the data as states
path = "D:/PhonePe Project/pulse/data/top/user/country/india/state"
Agg_state_list = os.listdir(path)

# This is to extract the data to create a dataframe
columns_6 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'RegisteredUsers': []}

for state in Agg_state_list:
    p_i = os.path.join(path, state)

    Agg_yr = os.listdir(p_i)
    for year in Agg_yr:
        p_j = os.path.join(p_i, year)
        Agg_yr_list = os.listdir(p_j)
        for file in Agg_yr_list:
            p_k = os.path.join(p_j, file)
            with open(p_k, 'r') as Data:
                try:
                    D = json.load(Data)

                    for pincode_data in D['data']['pincodes']:
                        name = pincode_data['name']
                        registeredUsers = pincode_data['registeredUsers']

                        columns_6['Pincode'].append(name)
                        columns_6['RegisteredUsers'].append(registeredUsers)
                        columns_6['State'].append(state)
                        columns_6['Year'].append(year)
                        columns_6['Quarter'].append(int(file.strip('.json')))
                except Exception as e:
                    print(f"An error occurred while processing {p_k}: {e}")

# Successfully created a dataframe
Agg_Trans = pd.DataFrame(columns_6)
Agg_Trans


# MYSQL :
AGGREGATED TRANSACTION

import mysql.connector

# Connect to MySQL
mysql_db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Shine@123',
    database='phonepe',
    auth_plugin="mysql_native_password"
)

# Create a MySQL cursor
mysql_cursor = mysql_db.cursor()

# Create the table 
table_creation_query = """
CREATE TABLE AGGREGATED_TRANSACTION (
    id INT AUTO_INCREMENT PRIMARY KEY,
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    Transaction_type VARCHAR(255),
    Transaction_count INT,
    Transaction_amount FLOAT
);
"""
mysql_cursor.execute(table_creation_query)

# Insert data into MySQL table
for index, row in Agg_Trans.iterrows():
    data_to_insert = (
        row["State"],
        row["Year"],
        row["Quarter"],
        row["Transaction_type"],
        row["Transaction_count"],
        row["Transaction_amount"]
    )
    insert_query = "INSERT INTO AGGREGATED_TRANSACTION (State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount) VALUES (%s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(insert_query, data_to_insert)

# Commit the changes and close the connection
mysql_db.commit()
mysql_db.close()

print("Data successfully inserted into MySQL.")


# AGGREGATED USER  :

import mysql.connector

# Connect to MySQL
mysql_db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Shine@123',
    database='phonepe',
    auth_plugin="mysql_native_password"
)

# Create a MySQL cursor
mysql_cursor = mysql_db.cursor()

# Create the table 
table_creation_query = """
CREATE TABLE `aggregated_user` (
    id INT AUTO_INCREMENT PRIMARY KEY,
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    Brands VARCHAR(255),
    Count INT,
    Percentage FLOAT
);
"""

mysql_cursor.execute(table_creation_query)

# Insert data into MySQL table
for index, row in Agg_Brands.iterrows():
    data_to_insert = (
        row["State"],
        row["Year"],
        row["Quarter"],
        row["Brands"],
        row["Count"],
        row["Percentage"]
    )
    insert_query = "INSERT INTO `aggregated_user` (State, Year, Quarter, Brands, Count, Percentage) VALUES (%s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(insert_query, data_to_insert)


# Commit the changes and close the connection
mysql_db.commit()
mysql_db.close()

print("Data successfully inserted into MySQL.")


# MAP TRANSACTION :


import mysql.connector

# Connect to MySQL

mysql_db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Shine@123',
    database='phonepe',
    auth_plugin="mysql_native_password"
)

# Create a cursor
mysql_cursor = mysql_db.cursor()

# Create the table if it doesn't exist
table_creation_query = """
CREATE TABLE IF NOT EXISTS `MAP_TRANSACTION` (
    id INT AUTO_INCREMENT PRIMARY KEY,
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    District VARCHAR(255),
    Count INT,
    Amount FLOAT
)
"""
mysql_cursor.execute(table_creation_query)

# Insert data into MySQL table
for index, row in Agg_Trans.iterrows():
    data_to_insert = (
        row['State'],
        row['Year'],
        row['Quarter'],
        row['District'],
        row['Count'],
        row['Amount']
    )
    insert_query = "INSERT INTO `MAP_TRANSACTION` (State, Year, Quarter, District, Count, Amount) VALUES (%s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(insert_query, data_to_insert)

# Commit the changes
mysql_db.commit()

# Close the connection
mysql_db.close()

print("Data successfully inserted into MySQL.")

# MAP USER :


import mysql.connector

# Connect to MySQL
mysql_db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Shine@123',
    database='phonepe',
    auth_plugin="mysql_native_password"
)

# Create a cursor
mysql_cursor = mysql_db.cursor()

# Create the table if it doesn't exist
table_creation_query = """
CREATE TABLE IF NOT EXISTS map_user (
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    District VARCHAR(255),
    RegisteredUser INT,
    AppOpens INT
)
"""
mysql_cursor.execute(table_creation_query)

# Insert data into MySQL table
for index, row in Agg_Trans.iterrows():
    data_to_insert = (
        row['State'],
        row['Year'],
        row['Quarter'],
        row['District'],
        row['RegisteredUser'],
        row['AppOpens']
    )
    insert_query = "INSERT INTO map_user (State, Year, Quarter, District, RegisteredUser, AppOpens) VALUES (%s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(insert_query, data_to_insert)

# Commit the changes and close the connection
mysql_db.commit()
mysql_db.close()

print("Data successfully inserted into MySQL.")

# TOP TRANSACTION :

import mysql.connector

# MySQL connection setup
mysql_db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Shine@123',
    database='phonepe',
    auth_plugin="mysql_native_password"
)

# Create a cursor
mysql_cursor = mysql_db.cursor()

# Create the table if it doesn't exist
table_creation_query = """
CREATE TABLE if not exists top_transaction (
    State varchar(50),
    Year int,
    Quarter int,
    Pincode varchar(50),
    Transaction_count bigint,
    Transaction_amount bigint
)
"""
mysql_cursor.execute(table_creation_query)

# Insert data into MySQL table
for index, row in Agg_Trans.iterrows():
    data_to_insert = (
        row['State'],
        row['Year'],
        row['Quarter'],
        row['Pincode'],
        row['Transaction_count'],
        row['Transaction_amount']
    )
    insert_query = "INSERT INTO top_transaction (State, Year, Quarter, Pincode, Transaction_count, Transaction_amount) VALUES (%s,%s,%s,%s,%s,%s)"
    mysql_cursor.execute(insert_query, data_to_insert)

# Commit the changes and close the connection
mysql_db.commit()
mysql_db.close()

print("Data successfully inserted into MySQL.")

# TOP USER :


import mysql.connector

# MySQL connection setup

mysql_db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Shine@123',
    database='phonepe',
    auth_plugin="mysql_native_password"
)



# Create a cursor
mysql_cursor = mysql_db.cursor()

# Create the table if it doesn't exist
table_creation_query = """
CREATE TABLE if not exists top_user (
    State varchar(50),
    Year int,
    Quarter int,
    Pincode varchar(50),
    RegisteredUsers bigint
)
"""
mysql_cursor.execute(table_creation_query)

# Insert data into MySQL table
for index, row in Agg_Trans.iterrows():
    data_to_insert = (
        row['State'],
        row['Year'],
        row['Quarter'],
        row['Pincode'],
        row['RegisteredUsers']
    )
    insert_query = "INSERT INTO top_user (State, Year, Quarter, Pincode, RegisteredUsers) VALUES (%s,%s,%s,%s,%s)"
    mysql_cursor.execute(insert_query, data_to_insert)

# Commit the changes and close the connection
mysql_db.commit()
mysql_db.close()

print("Data successfully inserted into MySQL.")


##################################################

import streamlit as st
import pandas as pd
import plotly.express as px
import mysql.connector

# Connect to MySQL
mysql_db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Shine@123',
    database='phonepe',
    auth_plugin="mysql_native_password"
)

# Create a MySQL cursor
mysql_cursor = mysql_db.cursor()

# Streamlit App
st.title("PhonePe Analytics Dashboard")

# Create buttons for each category
selected_category = st.selectbox("Select Category", ["AGGREGATED TRANSACTION", "AGGREGATED USER", "MAP TRANSACTION", "MAP USER", "TOP TRANSACTION", "TOP USER"])

# Create dropdown for selecting year
selected_year = st.selectbox("Select Year", list(range(2018, 2025)))

# Create dropdown for selecting quarter
selected_quarter = st.selectbox("Select Quarter", [1, 2, 3, 4])

# Function to fetch data from MySQL based on category, year, and quarter
def fetch_data(category, year, quarter):
    query = f"SELECT * FROM {category.replace(' ', '_')} WHERE Year = {year} AND Quarter = {quarter}"
    
    # Execute the query
    mysql_cursor.execute(query)

    # Fetch all the rows
    data = mysql_cursor.fetchall()

    # Create a DataFrame from the fetched data
    df = pd.DataFrame(data, columns=[desc[0] for desc in mysql_cursor.description])

    return df

# Display data and create Plotly chart based on the selected category
st.subheader(f"{selected_category} Data")

# Select only the top 10 rows for display
data_display = fetch_data(selected_category, selected_year, selected_quarter).head(10)
st.write(data_display)

# Create Plotly charts based on the selected category
fig_bar = None
fig_pie = None

if not data_display.empty:
    st.subheader(f"{selected_category} Charts")
    
    if selected_category == "AGGREGATED TRANSACTION":
        fig_bar = px.bar(data_display, x="Transaction_type", y="Transaction_count", color="Transaction_type", title="Transaction Count by Type")
        fig_pie = px.pie(data_display, names="Transaction_type", values="Transaction_count", title="Transaction Count Distribution by Type")
    
    elif selected_category == "AGGREGATED USER":
        fig_bar = px.bar(data_display, x="Brands", y="Count", color="Brands", title="User Distribution by Brands")
        fig_pie = px.pie(data_display, names="Brands", values="Count", title="User Distribution by Brands")
    
    elif selected_category == "MAP TRANSACTION":
        fig_bar = px.bar(data_display, x="State", y="Amount", color="State", title="Transaction Amount by State on India Map")
        fig_pie = px.pie(data_display, names="State", values="Amount", title="Transaction Amount Distribution by State")
    
    elif selected_category == "MAP USER":
        fig_bar = px.bar(data_display, x="State", y="AppOpens", color="State", title="App Opens by State on India Map")
        fig_pie = px.pie(data_display, names="State", values="AppOpens", title="App Opens Distribution by State")
    
    elif selected_category == "TOP TRANSACTION":
        fig_bar = px.bar(data_display, x="Pincode", y="Transaction_count", color="Pincode", title="Transaction Count by Pincode")
        fig_pie = px.pie(data_display, names="Pincode", values="Transaction_count", title="Transaction Count Distribution by Pincode")
    
    elif selected_category == "TOP USER":
        fig_bar = px.bar(data_display, x="Pincode", y="RegisteredUsers", color="Pincode", title="Registered Users Count by Pincode")
        fig_pie = px.pie(data_display, names="Pincode", values="RegisteredUsers", title="Registered Users Distribution by Pincode")

# Display the Bar Chart
if fig_bar is not None:
    st.subheader(f"{selected_category} Bar Chart")
    st.plotly_chart(fig_bar)

# Create a button for the pie chart
if st.button("Show Pie Chart"):
    st.subheader(f"{selected_category} Pie Chart")
    
    if fig_pie is not None:
        st.plotly_chart(fig_pie)

# Close the MySQL connection
mysql_db.close()



###############################


import streamlit as st
import pandas as pd
import plotly.express as px

# Sample data (replace this with your actual state-wise data)
state_data = {
    "State": ["Andaman & Nicobar", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chandigarh",
              "Chhattisgarh", "Dadra and Nagar Haveli and Daman and Diu", "Delhi", "Goa", "Gujarat", "Haryana",
              "Himachal Pradesh", "Jammu & Kashmir", "Jharkhand", "Karnataka", "Kerala", "Ladakh", "Madhya Pradesh",
              "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Puducherry", "Punjab",
              "Rajasthan", "Sikkim", "Telangana", "Tripura", "Uttarakhand", "Uttar Pradesh", "West Bengal"],
    
    "Total amount": [8.69562, 6.25415, 4.20250, 1.060142, 2.82952, 164, 2.92721, 9.5947, 7.32405, 6.36777, 5.71923,
                     0.247033, 0.214392, 0.5488, 2069, 30661, 176, 5562, 114947, 0.5678, 309, 112, 525, 4436, 774, 2587,
                     6666, 0.4455, 46717, 13327, 676, 937, 15720, 13679],
    
    "Lowest amount": [7.845307, 4.213866, 1.525072, 2.060142, 9.65343, 164, 5.004404, 8.310501, 3.920985, 2.56780, 4.33445,
                     0.247033, 0.214392, 0.5488, 2069, 30661, 176, 5562, 114947, 0.5678, 309, 112, 525, 4436, 774, 2587,
                     6666, 0.4455, 46717, 13327, 676, 937, 15720, 13679],
    
    "Highest amount": [3.845307, 4.213866, 3.525072, 9.060142, 2.65343, 164, 1.004404, 8.310501, 3.920985, 2.56780, 4.33445,
                     0.47033, 0.214392, 0.5488, 2069, 30661, 176, 5562, 114947, 0.5678, 309, 112, 525, 4436, 774, 2587,
                     6666, 0.4455, 46717, 13327, 676, 937, 15720, 13679],
    
    "Lowest Transaction amount": [4.845307, 9.213866, 4.525072, 1.060142, 1.65343, 164, 1.004404, 4.310501, 3.920985, 2.56780, 4.33445,
                     0.247033, 0.214392, 0.5488, 2069, 30661, 176, 5562, 114947, 0.5678, 309, 112, 525, 4436, 774, 2587,
                     6666, 0.4455, 46717, 13327, 676, 937, 15720, 13679],
    
    "Totalapp open": [1.845307, 2.213866, 4.525072, 4.060142, 9.65343, 164, 1.004404, 2.310501, 3.920985, 2.56780, 4.33445,
                     0.247033, 0.214392, 0.5488, 2069, 30661, 176, 5562, 114947, 0.5678, 309, 112, 525, 4436, 774, 2587,
                     6666, 0.4455, 46717, 13327, 676, 937, 15720, 13679],
    
    "Lowest Transaction count": [3.845307, 9.213866, 4.525072, 9.060142, 4.65343, 164, 9.004404, 1.310501, 3.920985, 2.56780, 4.33445,
                     0.247033, 0.214392, 0.5488, 2069, 30661, 176, 5562, 114947, 0.5678, 309, 112, 525, 4436, 774, 2587,
                     6666, 0.4455, 46717, 13327, 676, 937, 15720, 13679],
    
    "Highest Transaction count": [6.845307, 7.213866, 1.525072, 1.060142, 1.65343, 164, 4.004404, 8.310501, 3.920985, 2.56780, 4.33445,
                     0.247033, 0.214392, 0.5488, 2069, 30661, 176, 5562, 114947, 0.5678, 309, 112, 525, 4436, 774, 2587,
                     6666, 0.4455, 46717, 13327, 676, 937, 15720, 13679],
    
    "Average Transaction amount": [1.845307, 1.213866, 4.525072, 1.060142, 1.65343, 164, 2.004404, 8.310501, 3.920985, 2.56780, 4.33445,
                     0.247033, 0.214392, 0.5488, 2069, 30661, 176, 5562, 114947, 0.5678, 309, 112, 525, 4436, 774, 2587,
                     6666, 0.4455, 46717, 13327, 676, 937, 15720, 13679],
    
    "Average Transaction count": [3.845307, 5.213866, 4.525072, 1.060142, 7.65343, 164, 8.004404, 9.310501, 3.920985, 2.56780, 4.33445,
                     0.247033, 0.214392, 0.5488, 2069, 30661, 176, 5562, 114947, 0.5678, 309, 112, 525, 4436, 774, 2587,
                     6666, 0.4455, 46717, 13327, 676, 937, 15720, 13679],
}

df_state = pd.DataFrame(state_data)

# Streamlit App
def main():
    st.title("Choropleth Map Visualization")

    # Get the selected question from the user
    questions = [
        "Q1: Total amount",
        "Q2: Lowest amount",
        "Q3: Highest amount",
        "Q4: Lowest Transaction amount",
        "Q5: Totalapp open",
        "Q6: Lowest Transaction count",
        "Q7: Highest Transaction count",
        "Q8: Average Transaction amount",
        "Q9: Average Transaction count",
        "Q10: Average Totalapp open count",  # Replace the custom question with a new one
    ]

    selected_question = st.sidebar.selectbox("Choose a Question", questions)

    # Display the selected question
    st.header(selected_question)

    # Display the data table
    st.write("Data Table:")
    if selected_question != questions[-1]:  # Check if the selected question is not the custom question
        st.write(df_state[["State", selected_question.split(":")[1].strip()]])
    else:
        # Custom question logic goes here
        custom_column_name = "Average Totalapp open count"
        df_state[custom_column_name] = df_state["Totalapp open"].mean()
        st.write(df_state[["State", custom_column_name]])

    # Choropleth map for the selected question
    fig = create_choropleth(df_state, selected_question.split(":")[1].strip(), f'Choropleth Map - {selected_question}')
    # Display the map
    st.write("Choropleth Map:")
    st.plotly_chart(fig)

def create_choropleth(df, color_column, title):
    fig = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color=color_column,
        color_continuous_scale='Reds',
        title=title
    )

    fig.update_geos(fitbounds="locations", visible=False)

    return fig

if __name__ == "__main__":
    main()


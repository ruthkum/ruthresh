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


##################################################  10 Quries

import streamlit as st
import mysql.connector
import pandas as pd
import requests
import json
import plotly.express as px

# Function to execute MySQL queries and return a DataFrame
def execute_query(query):
    mysql_db = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Shine@123',
        database='phonepe',
        auth_plugin="mysql_native_password"
    )

    mysql_cursor = mysql_db.cursor()
    mysql_cursor.execute(query)
    data = mysql_cursor.fetchall()
    columns = [i[0] for i in mysql_cursor.description]
    df = pd.DataFrame(data, columns=columns)
    mysql_db.close()
    return df

# Streamlit app
st.title("PhonePe Analytics Dashboard")

# Queries for 10 questions
queries = {
    "Top Brands Of Mobiles Used": "SELECT Brands, SUM(Count) AS TotalCount FROM aggregated_user GROUP BY Brands ORDER BY TotalCount DESC LIMIT 10",
    "States With Lowest Transaction Amount": "SELECT State, MIN(Transaction_amount) AS LowestAmount FROM AGGREGATED_TRANSACTION GROUP BY State",
    "Districts With Highest Transaction Amount": "SELECT District, MAX(Amount) AS HighestAmount FROM MAP_TRANSACTION GROUP BY District ORDER BY HighestAmount DESC LIMIT 1",
    "Top 10 Districts With Lowest Transaction Amount": "SELECT District, MIN(Amount) AS LowestAmount FROM MAP_TRANSACTION GROUP BY District ORDER BY LowestAmount ASC LIMIT 10",
    "Top 10 States With AppOpens": "SELECT State, SUM(AppOpens) AS TotalAppOpens FROM map_user GROUP BY State ORDER BY TotalAppOpens DESC LIMIT 10",
    "Least 10 States With AppOpens": "SELECT State, SUM(AppOpens) AS TotalAppOpens FROM map_user GROUP BY State ORDER BY TotalAppOpens ASC LIMIT 10",
    "States With Lowest Transaction Count": "SELECT State, MIN(Transaction_count) AS LowestCount FROM top_transaction GROUP BY State",
    "States With Highest Transaction Count": "SELECT State, MAX(Transaction_count) AS HighestCount FROM top_transaction GROUP BY State",
    "States With Highest Transaction Amount": "SELECT State, MAX(Transaction_amount) AS HighestAmount FROM top_transaction GROUP BY State",
    "Top 50 Districts With Lowest Transaction Amount": "SELECT District, MIN(amount) AS LowestAmount FROM MAP_TRANSACTION GROUP BY District ORDER BY LowestAmount ASC LIMIT 50"
}

# Choose the question to display
selected_question = st.selectbox("Select a question:", list(queries.keys()))

# Display result based on selected question
result_df = execute_query(queries[selected_question])
st.dataframe(result_df)

# Visualization button for Line Plot, Pie Chart, and Bar Chart
if st.button("Show Visualizations"):
    # Display the Pie Chart
    st.subheader("Pie Chart Visualization")
    fig_pie = px.pie(result_df, values=result_df.columns[1], names=result_df.columns[0], title=selected_question)
    st.plotly_chart(fig_pie)

    # Display the Bar Chart
    st.subheader("Bar Chart Visualization")
    fig_bar = px.bar(result_df, x=result_df.columns[0], y=result_df.columns[1], title=selected_question)
    st.plotly_chart(fig_bar)

    # Display the Line Graph
    st.subheader("Line Graph Visualization")
    fig_line = px.line(result_df, x=result_df.columns[0], y=result_df.columns[1], title=selected_question)
    st.plotly_chart(fig_line)



################################   6  MAP VISUALISATION
    

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

# Function to fetch data from MySQL for a specific table and return a DataFrame
def fetch_mysql_data(table_name, column_name):
    query = f"SELECT state, {column_name} FROM {table_name}"
    mysql_cursor.execute(query)
    data = mysql_cursor.fetchall()
    columns = [desc[0] for desc in mysql_cursor.description]
    df = pd.DataFrame(data, columns=columns)
    return df

# Define the list of state names
state_data = {
    "State": ["Andaman & Nicobar", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chandigarh",
              "Chhattisgarh", "Dadra and Nagar Haveli and Daman and Diu", "Delhi", "Goa", "Gujarat", "Haryana",
              "Himachal Pradesh", "Jammu & Kashmir", "Jharkhand", "Karnataka", "Kerala", "Ladakh", "Madhya Pradesh",
              "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Puducherry", "Punjab",
              "Rajasthan", "Sikkim", "Telangana", "Tripura", "Uttarakhand", "Uttar Pradesh", "West Bengal"]
}

# Streamlit App
def main():
    st.title("Choropleth Map Visualization")

    # Define the table names and their corresponding column names
    tables = {
        "aggregated_transaction": "transaction_amount",
        "aggregated_user": "brands",
        "map_transaction": "amount",
        "map_user": "registereduser",
        "top_transaction": "transaction_amount",
        "top_user": "registeredusers"
    }

    # Create buttons for each question
    selected_question = st.sidebar.radio("Choose a Question", list(tables.keys()))

    # Fetch data from MySQL for the selected question
    df_states = pd.DataFrame(state_data)
    df_table = fetch_mysql_data(selected_question, tables[selected_question])
    df_states[selected_question.capitalize()] = df_table[tables[selected_question]]

    # Display the data table
    st.write("Data Table:")
    st.write(df_states)

    # Choropleth map for the selected question
    fig = create_choropleth(df_states, selected_question.capitalize(), f'Choropleth Map - {selected_question.capitalize()}')
    st.write(f"Choropleth Map - {selected_question.capitalize()}:")
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

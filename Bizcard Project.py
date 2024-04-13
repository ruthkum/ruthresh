################################ TRIAL IMAGE 1

import easyocr
import pandas as pd

# Define function to extract text from image using EasyOCR
def extract_text(image_path):
    try:
        # Initialize EasyOCR reader
        reader = easyocr.Reader(['en'])
        # Read text from image
        result = reader.readtext(image_path)
        # Extract text
        text_data = [entry[1] for entry in result]
        return text_data
    except Exception as e:
        print(f"Error occurred while extracting text from image: {e}")
        return []

# Given image path
image_path = "/content/1.png"

# Extract text from image
text_data = extract_text(image_path)

# Define column names
columns = ["NAME", "DESIGNATION", "MOBILE", "MOBILE 1",  "WEBSITE", "EMAIL", "DISTRICT", "NAME1","STATE","COMPANY"]

# Create DataFrame
df = pd.DataFrame([text_data[:len(columns)]], columns=columns)

# Transpose DataFrame to get vertical output
vertical_df = df.T.reset_index()

# Rename columns
vertical_df.columns = ['Attribute', 'Value']

# Display vertical DataFrame
print(vertical_df)

#################### IMAGE 1  (Sqlite 3) :

import sqlite3
import easyocr
import pandas as pd

# Define function to extract text from image using EasyOCR
def extract_text(image_path):
    try:
        # Initialize EasyOCR reader
        reader = easyocr.Reader(['en'])
        # Read text from image
        result = reader.readtext(image_path)
        # Extract text
        text_data = [entry[1] for entry in result]
        return text_data
    except Exception as e:
        print(f"Error occurred while extracting text from image: {e}")
        return []

# Given image path
image_path = "/content/1.png"

# Extract text from image
text_data = extract_text(image_path)

# Define column names
columns = ["NAME", "DESIGNATION", "MOBILE", "MOBILE 1",  "WEBSITE", "EMAIL", "DISTRICT", "NAME1","STATE","COMPANY"]

# Create DataFrame
df = pd.DataFrame([text_data[:len(columns)]], columns=columns)

# Transpose DataFrame to get vertical output
vertical_df = df.T.reset_index()

# Rename columns
vertical_df.columns = ['Attribute', 'Value']

# Create SQLite3 connection
conn = sqlite3.connect("image1.db")

# Define table name
table_name = "image_data"

# Create table if not exists
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (Attribute TEXT, Value TEXT)"
conn.execute(create_table_query)

# Insert data into table
for index, row in vertical_df.iterrows():
    conn.execute(f"INSERT INTO {table_name} (Attribute, Value) VALUES (?, ?)", (row['Attribute'], row['Value']))

# Commit changes and close connection
conn.commit()
conn.close()

print("Data inserted into SQLite3 database successfully.")


###########################  TRIAL Image 2 :

import easyocr
import pandas as pd

# Define function to extract text from image using EasyOCR
def extract_text(image_path):
    try:
        # Initialize EasyOCR reader
        reader = easyocr.Reader(['en'])
        # Read text from image
        result = reader.readtext(image_path)
        # Extract text
        text_data = [entry[1] for entry in result]
        return text_data
    except Exception as e:
        print(f"Error occurred while extracting text from image: {e}")
        return []

# Given image path
image_path = "/content/2.png"

# Extract text from image
text_data = extract_text(image_path)

# Define column names
columns = ["NAME", "DESIGNATION", "MOBILE", "EMAIL","WEB", "WEBSITE","ADDRESS", "DISTRICT", "COMPANY", "STATE",]

# Create DataFrame
df = pd.DataFrame([text_data[:len(columns)]], columns=columns)

# Transpose DataFrame to get vertical output
vertical_df = df.T.reset_index()

# Rename columns
vertical_df.columns = ['Attribute', 'Value']

# Display vertical DataFrame
print(vertical_df)


######################## Sqlite 3  (image 2)

import sqlite3

# SQLite database path
db_path = "image2.db"

# Table name
table_name = "image_data"

# Create SQLite3 connection
conn = sqlite3.connect(db_path)

# Define column names
columns = ["NAME", "DESIGNATION", "MOBILE", "EMAIL", "WEB", "WEBSITE", "ADDRESS", "DISTRICT", "COMPANY", "STATE"]

# Create table if not exists
conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{column} TEXT' for column in columns])})")

# Insert data into table
placeholders = ', '.join(['?' for _ in columns])
insert_data_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
conn.execute(insert_data_query, list(df.iloc[0]))

# Commit changes and close connection
conn.commit()
conn.close()

print("Data inserted into SQLite3 database successfully.")


############################## TRIAL  Image 3 :

import easyocr
import pandas as pd

# Define function to extract text from image using EasyOCR
def extract_text(image_path):
    try:
        # Initialize EasyOCR reader
        reader = easyocr.Reader(['en'])
        # Read text from image
        result = reader.readtext(image_path)
        # Extract text
        text_data = [entry[1] for entry in result]
        return text_data
    except Exception as e:
        print(f"Error occurred while extracting text from image: {e}")
        return []

# Given image path
image_path = "/content/3.png"

# Extract text from image
text_data = extract_text(image_path)

# Define column names
columns = ["NAME", "DESIGNATION", "DISTRICT", "STATE", "MOBILE", "EMAIL", "WEBSITE", "COMPANY", "TRANSPORT"]

# Create DataFrame
df = pd.DataFrame([text_data[:len(columns)]], columns=columns)

# Transpose DataFrame to get vertical output
vertical_df = df.T.reset_index()

# Rename columns
vertical_df.columns = ['Attribute', 'Value']

# Display vertical DataFrame
print(vertical_df)

############################### SQlite 3 (Image 3):

import sqlite3

# SQLite database path
db_path = "image3.db"

# Table name
table_name = "image_data"

# Create SQLite3 connection
conn = sqlite3.connect(db_path)

# Create table if not exists
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (Attribute TEXT, Value TEXT)"
conn.execute(create_table_query)

# Insert data into table
for index, row in vertical_df.iterrows():
    conn.execute(f"INSERT INTO {table_name} (Attribute, Value) VALUES (?, ?)", (row['Attribute'], row['Value']))

# Commit changes and close connection
conn.commit()
conn.close()

print("Data inserted into SQLite3 database successfully.")


######################################  TRIAL IMAGE 4 :

import easyocr
import pandas as pd

# Define function to extract text from image using EasyOCR
def extract_text(image_path):
    try:
        # Initialize EasyOCR reader
        reader = easyocr.Reader(['en'])
        # Read text from image
        result = reader.readtext(image_path)
        # Extract text
        text_data = [entry[1] for entry in result]
        return text_data
    except Exception as e:
        print(f"Error occurred while extracting text from image: {e}")
        return []

# Given image path
image_path = "/content/4.png"

# Extract text from image
text_data = extract_text(image_path)

# Define column names
columns = ["NAME", "DESIGNATION", "STATE", "PINCODE", "MOBILE", "EMAIL","CATEGORY", "WEBSITE", "HOTEL TYPE", ]

# Create DataFrame
df = pd.DataFrame([text_data[:len(columns)]], columns=columns)

# Transpose DataFrame to get vertical output
vertical_df = df.T.reset_index()

# Rename columns
vertical_df.columns = ['Attribute', 'Value']

# Display vertical DataFrame
print(vertical_df)


########################################  SQite 3 (Image 4) :

import sqlite3

# SQLite database path
db_path = "image4.db"

# Table name
table_name = "image_data"

# Create SQLite3 connection
conn = sqlite3.connect(db_path)

# Create table if not exists
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (Attribute TEXT, Value TEXT)"
conn.execute(create_table_query)

# Insert data into table
for index, row in vertical_df.iterrows():
    conn.execute(f"INSERT INTO {table_name} (Attribute, Value) VALUES (?, ?)", (row['Attribute'], row['Value']))

# Commit changes and close connection
conn.commit()
conn.close()

print("Data inserted into SQLite3 database successfully.")


################################################  TRIAL (Image 5) :

import easyocr
import pandas as pd

# Define function to extract text from image using EasyOCR
def extract_text(image_path):
    try:
        # Initialize EasyOCR reader
        reader = easyocr.Reader(['en'])
        # Read text from image
        result = reader.readtext(image_path)
        # Extract text
        text_data = [entry[1] for entry in result]
        return text_data
    except Exception as e:
        print(f"Error occurred while extracting text from image: {e}")
        return []

# Given image path
image_path = "/content/5.png"

# Extract text from image
text_data = extract_text(image_path)

# Define column names
columns = ["NAME", "DESIGNATION", "STATE", "PINCODE", "MOBILE", "EMAIL", "WEBSITE", " COMPANY", ]

# Create DataFrame
df = pd.DataFrame([text_data[:len(columns)]], columns=columns)

# Transpose DataFrame to get vertical output
vertical_df = df.T.reset_index()

# Rename columns
vertical_df.columns = ['Attribute', 'Value']

# Display vertical DataFrame
print(vertical_df)


###################################################  SQlite (Image 5 ) :

import sqlite3

# SQLite database path
db_path = "image5.db"

# Table name
table_name = "image_data"

# Create SQLite3 connection
conn = sqlite3.connect(db_path)

# Create table if not exists
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (Attribute TEXT, Value TEXT)"
conn.execute(create_table_query)

# Insert data into table
for index, row in vertical_df.iterrows():
    conn.execute(f"INSERT INTO {table_name} (Attribute, Value) VALUES (?, ?)", (row['Attribute'], row['Value']))

# Commit changes and close connection
conn.commit()
conn.close()

print("Data inserted into SQLite3 database successfully.")


%%writefile demo.py

import streamlit as st
import sqlite3
import easyocr
import pandas as pd
from io import BytesIO

# Define function to extract text from image using EasyOCR
def extract_text(image_bytes, columns):
    try:
        # Initialize EasyOCR reader
        reader = easyocr.Reader(['en'])
        # Read text from image
        result = reader.readtext(image_bytes)
        # Extract text
        text_data = [entry[1] for entry in result]
        data = {col: text_data[i] if i < len(text_data) else "" for i, col in enumerate(columns)}
        return data
    except Exception as e:
        st.error(f"Error occurred while extracting text from image: {e}")
        return {}

# Function to create SQLite3 connection and insert data into table
def insert_into_sqlite(data):
    conn = sqlite3.connect("image_data.db")
    table_name = "image_data"
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (Attribute TEXT, Value TEXT)"
    conn.execute(create_table_query)
    for attribute, value in data.items():
        conn.execute(f"INSERT INTO {table_name} (Attribute, Value) VALUES (?, ?)", (attribute, value))
    conn.commit()
    conn.close()

# Function to update data in SQLite table
def update_data_in_sqlite(updated_data):
    conn = sqlite3.connect("image_data.db")
    for attribute, new_value in updated_data.items():
        conn.execute(f"UPDATE image_data SET Value = ? WHERE Attribute = ?", (new_value, attribute))
    conn.commit()
    conn.close()

# Function to delete data from SQLite table
def delete_data_from_sqlite(attribute):
    conn = sqlite3.connect("image_data.db")
    conn.execute(f"DELETE FROM image_data WHERE Attribute = ?", (attribute,))
    conn.commit()
    conn.close()

# Function to retrieve data from SQLite table
def get_data_from_sqlite():
    conn = sqlite3.connect("image_data.db")
    cursor = conn.execute("SELECT * FROM image_data")
    data = {row[0]: row[1] for row in cursor}
    conn.close()
    return data

# Streamlit UI
def main():
    st.title("Image Text Extraction and Database Insertion")

    # Create image_data table if it doesn't exist
    conn = sqlite3.connect("image_data.db")
    create_table_query = """CREATE TABLE IF NOT EXISTS image_data (
                                Attribute TEXT,
                                Value TEXT
                            );"""
    conn.execute(create_table_query)
    conn.close()

    # Image selection
    image_selection = st.selectbox("Select image:", ["Image 1", "Image 2", "Image 3", "Image 4", "Image 5"])

    # Define columns based on image selection
    if image_selection == "Image 1":
        columns = ["NAME", "DESIGNATION", "MOBILE", "MOBILE 1",  "WEBSITE", "EMAIL", "DISTRICT", "NAME1","STATE","COMPANY"]
    elif image_selection == "Image 2":
        columns = ["NAME", "DESIGNATION", "MOBILE", "EMAIL","WEB", "WEBSITE","ADDRESS", "DISTRICT", "COMPANY", "STATE"]
    elif image_selection == "Image 3":
        columns = ["NAME", "DESIGNATION", "DISTRICT", "STATE", "MOBILE", "EMAIL", "WEBSITE", "COMPANY", "TRANSPORT"]
    elif image_selection == "Image 4":
        columns = ["NAME", "DESIGNATION", "STATE", "PINCODE", "MOBILE", "EMAIL","CATEGORY", "WEBSITE", "HOTEL TYPE"]
    elif image_selection == "Image 5":
        columns = ["NAME", "DESIGNATION", "STATE", "PINCODE", "MOBILE", "EMAIL", "WEBSITE", "COMPANY"]

    data = {}  # Initialize data dictionary

    # Image upload
    uploaded_file = st.file_uploader("Upload an image", type=["jpg", "png", "jpeg"])

    if uploaded_file is not None:
        image_bytes = uploaded_file.read()  # Read image bytes
        image = st.image(BytesIO(image_bytes), caption='Uploaded Image', use_column_width=True)  # Display uploaded image
        data = extract_text(image_bytes, columns)  # Extract text from image bytes
        df = pd.DataFrame([data.values()], columns=data.keys())  # Create DataFrame
        st.dataframe(df)  # Display extracted text in DataFrame

        if st.button("Insert Data into Database"):
            insert_into_sqlite(data)  # Insert data into SQLite database
            st.success("Data inserted into SQLite3 database successfully.")

    # Application form to edit details
    st.subheader("Edit Details")
    updated_data = {}
    for attribute in data.keys():
        updated_data[attribute] = st.text_input(f"Enter new value for {attribute}", value=data.get(attribute, ""))
    if st.button("Update All"):
        update_data_in_sqlite(updated_data)
        st.success("All attributes updated successfully.")

    # Delete button
    st.subheader("Delete Data")
    attribute_to_delete = st.selectbox("Select attribute to delete:", ["", *data.keys()])
    if attribute_to_delete:
        if st.button("Delete"):
            delete_data_from_sqlite(attribute_to_delete)
            st.success(f"{attribute_to_delete} deleted successfully.")

    # Check Updated Data button
    if st.button("Check Updated Data"):
        updated_data = get_data_from_sqlite()
        if updated_data:
            updated_df = pd.DataFrame(updated_data.items(), columns=["Attribute", "Value"])
            st.subheader("Updated Data")
            st.dataframe(updated_df)
        else:
            st.warning("No updated data found.")

if __name__ == '__main__':
    main()




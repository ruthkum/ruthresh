
import requests
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
import mysql.connector
import streamlit as st

# API key
api_key = "AIzaSyCBF8YBbg4uvH33iDZAcMVS3xPIyUoXRJk"

# List of channel IDs for 10 different channels
channel_ids = [
    "UC8JT2m0mKEgvEtie3JNKwew",
    "UChGd9JY4yMegY6PxqpBjpRA",
    "UC5cY198GU1MQMIPJgMkCJ_Q",
    "UCGx7rPjOTx-Sm8u85KRI1wA",
    "UCY6KjrDBN_tIRFT_QNqQbRQ",
    "UCXnDDUQyJpRfC98_ZRIuhZA",
    "UCnjU1FHmao9YNfPzE039YTw",
    "UCpOnZdJQxa5vyR5dNtIoNjg",
    "UC-j7LP4at37y3uNTdWLq-vQ",
    "UCs3DSHP8I6rF9JASQNwf9sQ",
]

# Lists to store the data
channel_data_list = []
video_data_list = []

# Function to retrieve channel information
def get_channel_info(channel_id):
    channel_url = f"https://www.googleapis.com/youtube/v3/channels?key={api_key}&part=snippet,statistics,brandingSettings&id={channel_id}"
    response = requests.get(channel_url)
    data = response.json()
    return data

# Function to retrieve video information for a channel
def get_video_info(channel_id):
    playlist_url = f"https://www.googleapis.com/youtube/v3/playlists?key={api_key}&part=snippet&channelId={channel_id}"
    response = requests.get(playlist_url)
    data = response.json()

    if "items" in data and len(data["items"]) > 0:
        playlist_id = data["items"][0]["id"]
        video_url = f"https://www.googleapis.com/youtube/v3/playlistItems?key={api_key}&part=snippet&maxResults=10&playlistId={playlist_id}"
        response = requests.get(video_url)
        data = response.json()
        return data.get("items", []), playlist_id  # Return video data and playlist ID

    return [], None  # Return empty list and None for playlist ID

# Function to retrieve video statistics (views and duration)
def get_video_statistics(video_id):
    video_url = f"https://www.googleapis.com/youtube/v3/videos?key={api_key}&part=contentDetails,statistics&id={video_id}"
    response = requests.get(video_url)
    data = response.json()

    try:
        statistics = data.get("items", [])[0]["statistics"]
        content_details = data.get("items", [])[0]["contentDetails"]
        return statistics, content_details
    except IndexError:
        return None, None  # Return None values to handle the case when the list is empty

# Function to retrieve video comments
def get_video_comments(video_id):
    comments_url = f"https://www.googleapis.com/youtube/v3/commentThreads?key={api_key}&part=snippet&maxResults=10&videoId={video_id}"
    response = requests.get(comments_url)
    data = response.json()

    comments = []
    for item in data.get("items", []):
        comment = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
        comments.append(comment)

    return comments

# Connect to MySQL
mysql_db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Shine@123',
    database='youtubedb1',
    auth_plugin="mysql_native_password"
)
mysql_cursor = mysql_db.cursor()

# Loop through the list of channel IDs
for channel_id in channel_ids:
    channel_info = get_channel_info(channel_id)
    video_info, playlist_id = get_video_info(channel_id)  # Fetch video info and playlist ID

    # Process channel information
    if "items" in channel_info and len(channel_info["items"]) > 0:
        channel_data = channel_info["items"][0]
        channel_name = channel_data["snippet"]["title"]
        channel_description = channel_data["brandingSettings"]["channel"].get("description", "Description not available")
        subscribers = channel_data["statistics"]["subscriberCount"]
        total_videos = channel_data["statistics"]["videoCount"]
        PublishedAt = channel_data['snippet']['publishedAt']
        Published_Date = channel_data['snippet']['publishedAt']

        channel_data_list.append({
            "Channel Name": channel_name,
            "Channel Description": channel_description,
            "Channel ID": channel_id,
            "Subscribers": subscribers,
            "Total Video Count": total_videos,
        })

    # Process video information
    if video_info:
        for video in video_info:
            video_id = video["snippet"]["resourceId"]["videoId"]
            likes = video["snippet"]["position"]
            author = video["snippet"]["channelTitle"]
            published_at = video["snippet"]["publishedAt"]

            published_date = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
            published_month = published_date.strftime("%B %Y")

            statistics, content_details = get_video_statistics(video_id)
            comments = get_video_comments(video_id)

            # Include only the first comment in the DataFrame
            first_comment = comments[0] if comments else None

            # Handle None values from get_video_statistics
            views = statistics.get("viewCount", 0) if statistics else 0
            duration = content_details.get("duration", "PT0S") if content_details else "PT0S"

            comments = get_video_comments(video_id)

            video_data_list.append({
                "Video ID": video_id,
                "Likes": likes,
                "Author": author,
                "Published At": published_at,
                "Published Date": published_date.strftime("%Y-%m-%d"),
                "Published Month": published_month,
                "Views": views,
                "Duration": duration,
                "Comments": comments,
                "Playlist ID": playlist_id,  # New field for Playlist ID
            })

import requests
import pandas as pd
from datetime import datetime
from pymongo import MongoClient

# ... (Previous code remains unchanged)

# Create Pandas DataFrames
channel_df = pd.DataFrame(channel_data_list)
video_df = pd.DataFrame(video_data_list)

# Combine DataFrames into a single table
combined_df = pd.concat([channel_df, video_df], axis=1)

# Drop rows with NaN values
combined_df = combined_df.dropna()

# Reset the indices using df.reset_index()
combined_df = combined_df.reset_index(drop=True)

# Display the combined DataFrame
print("Combined Data:")
print(combined_df)

#####################################################

# MONGODB INSERT:

# MongoDB Atlas connection string
client = MongoClient('mongodb+srv://RUTHRESH:ruth123@cluster0.w4tiynw.mongodb.net/?retryWrites=true&w=majority')

# Access the database and collection
mongo_db = client["youtube_db"]
mongo_collection = mongo_db["youtube_demo1"]

# Convert combined DataFrame to a list of dictionaries
data_to_insert = combined_df.to_dict(orient='records')

# Check if data_to_insert is not empty before inserting into MongoDB
if data_to_insert:
    # Insert data into MongoDB collection
    mongo_collection.insert_many(data_to_insert)
    print("Data inserted into MongoDB Atlas collection.")
else:
    print("No data to insert into MongoDB.")

##############################################################
    
# MYSQL CHANNEL DETAILS:

from pymongo import MongoClient
import mysql.connector

# Connect to MongoDB
username = 'RUTHRESH'
password = 'ruth123'
client = MongoClient('mongodb+srv://RUTHRESH:ruth123@cluster0.w4tiynw.mongodb.net/?retryWrites=true&w=majority')

try:
    # Access the database and collection
    db = client["youtube_db"]
    collection = db["youtube_demo1"]

    # Connect to MySQL
    mysql_db = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Shine@123',
        database='youtubedb1',
        auth_plugin="mysql_native_password"
    )
    mysql_cursor = mysql_db.cursor()

    # Create MySQL table
    mysql_cursor.execute("""
        CREATE TABLE IF NOT EXISTS channel_info (
            Channel_Name VARCHAR(255),
            Channel_Description TEXT,
            Channel_ID VARCHAR(255),
            Subscribers INT,
            Total_Video_Count INT
        )
    """)

    # Fetch data from MongoDB
    mongo_data = collection.find()

    # Print fetched data for debugging
    for record in mongo_data:
        print(record)

        # Insert data into MySQL table
        channel_name = record.get("Channel Name")
        channel_description = record.get("Channel Description")
        channel_id = record.get("Channel ID")
        subscribers = record.get("Subscribers")
        total_video_count = record.get("Total Video Count")

        mysql_cursor.execute("""
            INSERT INTO channel_info
            (Channel_Name, Channel_Description, Channel_ID, Subscribers, Total_Video_Count)
            VALUES (%s, %s, %s, %s, %s)
        """, (channel_name, channel_description, channel_id, subscribers, total_video_count))

    mysql_db.commit()
    mysql_cursor.close()
    mysql_db.close()
    client.close()

    print("Data transferred from MongoDB to MySQL successfully.")

except Exception as e:
    print(f"Error: {e}")

#############################################################
    
# MYSQL VIDEO DETAILS:
    
import requests
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
import mysql.connector
import numpy as np



# Connect to MongoDB
client = MongoClient('mongodb+srv://RUTHRESH:ruth123@cluster0.w4tiynw.mongodb.net/?retryWrites=true&w=majority')

try:
    # Access the database and collection
    db = client["youtube_db"]
    collection = db["youtube_demo1"]

    # Connect to MySQL
    mysql_db = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Shine@123',
        database='youtubedb1',
        auth_plugin="mysql_native_password"
    )
    mysql_cursor = mysql_db.cursor()

    # Create MySQL table for video info
    mysql_cursor.execute("""
        CREATE TABLE IF NOT EXISTS videos_info (
            Channel_Name VARCHAR(255),
            Channel_ID VARCHAR(255),
            Video_ID VARCHAR(255),
            Likes INT,
            Comments TEXT,
            Playlist_ID VARCHAR(255),
            Published_At DATETIME,
            Author VARCHAR(255),
            Published_Date DATE,
            Published_Month VARCHAR(20),
            Views INT,
            Duration VARCHAR(20)
        )
    """)

    # Fetch video data from MongoDB
    video_data = collection.find()

    # Insert video data into MySQL table
    for video in video_data:
        channel_name = video.get("Channel Name")
        channel_id = video.get("Channel ID")
        video_id = video.get("Video ID")
        likes = video.get("Likes")
        comments = ", ".join(video.get("Comments", []))
        playlist_id = video.get("Playlist ID")

        # Check and replace NaN values with None
        for key, value in video.items():
            if isinstance(value, np.ndarray) and np.isnan(value).any():
                video[key] = None

        # Check if 'Published At' is not None before parsing
        published_at_str = video.get("Published At")
        published_at = datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%SZ") if published_at_str else None

        author = video.get("Author")
        published_date = video.get("Published Date")
        published_month = video.get("Published Month")
        views = video.get("Views")
        duration = video.get("Duration")

        mysql_cursor.execute("""
            INSERT INTO videos_info
            (Channel_Name, Channel_ID, Video_ID, Likes, Comments, Playlist_ID, Published_At, Author, Published_Date, Published_Month, Views, Duration)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (channel_name, channel_id, video_id, likes, comments, playlist_id, published_at, author, published_date, published_month, views, duration))

    mysql_db.commit()
    mysql_cursor.close()
    mysql_db.close()
    client.close()

    print("Video data transferred from MongoDB to MySQL successfully.")

except Exception as e:
    print(f"Error: {e}")

###########################################################
#################  MONGODB DATA :
    
    
import pandas as pd
from pymongo import MongoClient

def get_data_from_mongodb(collection_name):
    # Connect to MongoDB
    client = MongoClient('mongodb+srv://RUTHRESH:ruth123@cluster0.w4tiynw.mongodb.net/?retryWrites=true&w=majority')

    try:
        # Access the database and collection
        db = client["youtube_db"]
        collection = db[collection_name]

        # Fetch data from MongoDB
        cursor = collection.find()
        data = list(cursor)

        # Create a DataFrame from the MongoDB data
        mongo_df = pd.DataFrame(data)

        return mongo_df

    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error

    finally:
        client.close()

# Streamlit app
def main():
    
    st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
    st.title("Display MongoDB Data with Streamlit")

    # Display MongoDB data button
    show_mongodb_data = st.button("Show MongoDB Data")

    if show_mongodb_data:
        st.header("MongoDB Data")
        collection_name = "youtube_demo1"  # Replace with your collection name
        mongo_df = get_data_from_mongodb(collection_name)

        if not mongo_df.empty:
            st.write(mongo_df)
        else:
            st.write("No data found in MongoDB collection.")

if __name__ == "__main__":
    main()

######################################################################


# Full question answer query  (1 to 10):

import streamlit as st
import mysql.connector
import pandas as pd

# Function to connect to MySQL database
def connect_to_mysql():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='Shine@123',
        database='youtubedb1',
        auth_plugin="mysql_native_password"
    )

# Function to execute SQL queries and display results
def execute_query(query):
    try:
        mysql_db = connect_to_mysql()
        mysql_cursor = mysql_db.cursor()

        mysql_cursor.execute(query)
        result = mysql_cursor.fetchall()

        mysql_cursor.close()
        mysql_db.close()

        return result

    except mysql.connector.Error as e:
        st.error(f"Error executing query: {e}")
        return None

# Streamlit app
def main():
    st.title("MySQL Queries and Outputs")

    # Queries dictionary with questions and corresponding queries
    queries = {
        "1. What are the names of all the videos and their corresponding channels?":
            "SELECT Video_ID, Channel_Name FROM videos_info",
        
        "2. Which channels have the most number of videos, and how many videos do they have?":
            "SELECT Channel_Name, Total_Video_Count FROM channel_info ORDER BY Total_Video_Count DESC LIMIT 5",
        
        "3. What are the top 10 most viewed videos and their respective channels?":
            "SELECT Video_ID, Views, Channel_Name FROM videos_info ORDER BY Views DESC LIMIT 10",

        "4. How many Comments were made on each video, and what are their corresponding video names?":
            "SELECT Video_ID, COUNT(Comments) AS 'Number of Comments' FROM videos_info GROUP BY Video_ID",


        "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
            "SELECT videos_info.Channel_Name AS 'Channel Name', videos_info.Video_ID AS 'Video ID', videos_info.Likes AS 'Likes' FROM videos_info ORDER BY Likes DESC LIMIT 5",
        
        "7. What is the total number of views for each channel, and what are their corresponding channel names?":
            "SELECT videos_info.Channel_Name AS 'Channel Name', SUM(videos_info.Views) AS 'Total Views' FROM videos_info GROUP BY videos_info.Channel_Name",
        
        "8. What are the names of all the channels that have published videos in the year 2023?":
            "SELECT DISTINCT videos_info.Channel_Name AS 'Channel Name' FROM videos_info WHERE YEAR(videos_info.Published_Date) = 2023",
        
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos_info GROUP BY Channel_Name",

        "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
            "SELECT videos_info.Channel_Name AS 'Channel Name', videos_info.Video_ID AS 'Video ID', videos_info.Comments AS 'Comments' FROM videos_info ORDER BY LENGTH(videos_info.Comments) DESC LIMIT 5",
    }

    # Display buttons for each question
    selected_query = st.selectbox("Select a question:", list(queries.keys()))

    if st.button("Show Output"):
        result = execute_query(queries[selected_query])

        if result:
            st.header(f"Output for '{selected_query}':")
            st.write(pd.DataFrame(result))
        else:
            st.write("No data available or an error occurred.")

if __name__ == "__main__":
    main()


##############################################################################
    
###  YouTube Channel Details Lookup

import streamlit as st
import mysql.connector
import pandas as pd

# Function to connect to MySQL database
def connect_to_mysql():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='Shine@123',
        database='youtubedb1',
        auth_plugin="mysql_native_password"
    )

# Function to execute SQL queries and display results
def execute_query(query, params=None):
    try:
        mysql_db = connect_to_mysql()
        mysql_cursor = mysql_db.cursor()

        if params:
            mysql_cursor.execute(query, params)
        else:
            mysql_cursor.execute(query)

        result = mysql_cursor.fetchall()

        mysql_cursor.close()
        mysql_db.close()

        return result

    except mysql.connector.Error as e:
        st.error(f"Error executing query: {e}")
        return None

# Function to fetch channel details based on channel ID
def get_channel_details(channel_id):
    query_channel_details = """
        SELECT * FROM channel_info
        WHERE Channel_ID = %s
        LIMIT 1
    """
    params = (channel_id,)
    return execute_query(query_channel_details, params)

# Function to fetch video details based on channel ID
def get_video_details(channel_id):
    query_video_details = """
        SELECT Video_ID, Likes, Comments, Playlist_ID, Published_At, Author,
               Published_Date, Published_Month, Views, Duration
        FROM videos_info
        WHERE Channel_ID = %s
        LIMIT 1
    """
    params = (channel_id,)
    return execute_query(query_video_details, params)

# Streamlit app
def main():
    st.title("YouTube Channel Details Lookup")

    # User input for channel ID
    channel_id_input_key = "channel_id_input"
    channel_id_input = st.text_input("Enter Channel ID:", key=channel_id_input_key)

    # Button to fetch and display channel details
    show_channel_details_button_key = "show_channel_details_button"
    if st.button("Show Channel Details", key=show_channel_details_button_key):
        # Fetch and display channel details
        result_channel_details = get_channel_details(channel_id_input)

        if result_channel_details:
            # Display Channel Details in a DataFrame
            channel_df = pd.DataFrame(result_channel_details, columns=["Channel Name", "Channel Description", "Channel ID", "Subscribers", "Total Video Count"])
            st.header("Channel Details:")
            st.write(channel_df)

            # Fetch and display video details
            result_video_details = get_video_details(channel_id_input)

            if result_video_details:
                # Display Video Details in a DataFrame
                video_df = pd.DataFrame(result_video_details, columns=["Video_ID", "Likes", "Comments", "Playlist_ID", "Published_At", "Author", "Published_Date", "Published_Month", "Views", "Duration"])
                st.header("Video Details:")
                st.write(video_df)
            else:
                st.write(f"No video details found for Channel ID: {channel_id_input}.")
        else:
            st.write(f"No channel details found for Channel ID: {channel_id_input}.")

if __name__ == "__main__":
    main()


#################################################

######  MONGODB SUCCESFULL :
    
import requests
from datetime import datetime
from pymongo import MongoClient
import streamlit as st

# Function to retrieve channel information from YouTube API
def get_channel_info(channel_id):
    api_key = "AIzaSyCBF8YBbg4uvH33iDZAcMVS3xPIyUoXRJk"
    channel_url = f"https://www.googleapis.com/youtube/v3/channels?key={api_key}&part=snippet,statistics,brandingSettings&id={channel_id}"
    response = requests.get(channel_url)
    data = response.json()
    return data

# Function to connect to MongoDB
def connect_to_mongodb():
    return MongoClient('mongodb+srv://RUTHRESH:ruth123@cluster0.w4tiynw.mongodb.net/?retryWrites=true&w=majority')

# Function to insert data into MongoDB
def insert_into_mongodb(collection, data):
    try:
        collection.insert_one(data)
        st.success("MongoDB insertion successful!")
    except Exception as e:
        st.error(f"Error inserting data into MongoDB: {e}")

# Streamlit app
def main():
    st.title("MONGODB INSERT")

    # User input for channel ID
    channel_id_input = st.text_input("Enter Channel ID:")

    # Button to fetch and display channel details
    if st.button("Show Channel Details"):
        # Fetch channel details from YouTube API
        channel_info = get_channel_info(channel_id_input)

        if "items" in channel_info and len(channel_info["items"]) > 0:
            # Display Channel Details
            channel_data = channel_info["items"][0]
            channel_name = channel_data["snippet"]["title"]
            channel_description = channel_data["brandingSettings"]["channel"].get("description", "Description not available")
            subscribers = channel_data["statistics"]["subscriberCount"]
            total_videos = channel_data["statistics"]["videoCount"]
            PublishedAt = channel_data['snippet']['publishedAt']
            Published_Date = channel_data['snippet']['publishedAt']

            st.header("Channel Details:")
            st.write(f"Channel Name: {channel_name}")
            st.write(f"Channel Description: {channel_description}")
            st.write(f"Subscribers: {subscribers}")
            st.write(f"Total Video Count: {total_videos}")
            st.write(f"Published At: {PublishedAt}")
            st.write(f"Published Date: {Published_Date}")

            # Connect to MongoDB and insert data
            mongo_client = connect_to_mongodb()
            mongo_db = mongo_client["youtube_db"]
            mongo_collection = mongo_db["youtube_channel_data"]

            # Create a dictionary with channel details
            channel_data_mongo = {
                "Channel Name": channel_name,
                "Channel Description": channel_description,
                "Channel ID": channel_id_input,
                "Subscribers": subscribers,
                "Total Video Count": total_videos,
                "Published At": PublishedAt,
                "Published Date": Published_Date
            }

            # Button to insert data into MongoDB
            if st.button("Insert into MongoDB"):
                insert_into_mongodb(mongo_collection, channel_data_mongo)

            # Close MongoDB connection
            mongo_client.close()

        else:
            st.write(f"No channel details found for Channel ID: {channel_id_input}.")

    # Button to display "MongoDB insert successfully" message
    if st.button("Insert Success Message"):
        st.success("MongoDB insert successfully!")

if __name__ == "__main__":
    main()

############################################
########  MYSQL SUCCESSFULL :
    
import streamlit as st
import mysql.connector
from pymongo import MongoClient
import uuid
import datetime

# Function to insert data into MySQL
def insert_into_mysql(cursor, data):
    try:
        # Convert '2023-11-19T17:42:13Z' to '2023-11-19 17:42:13'
        published_at_str = data["Published At"].replace("T", " ").replace("Z", "")
        published_at = datetime.datetime.strptime(published_at_str, "%Y-%m-%d %H:%M:%S")

        query = "INSERT INTO your_table_name (channel_name, channel_description, channel_id, subscribers, total_videos, published_at, published_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(query, (data["Channel Name"], data["Channel Description"], data["Channel ID"], data["Subscribers"], data["Total Video Count"], published_at, data["Published Date"]))
        st.success("MySQL insert successfully!")
    except Exception as e:
        st.error(f"Error inserting data into MySQL: {e}")

# Connect to MongoDB
client = MongoClient('mongodb+srv://RUTHRESH:ruth123@cluster0.w4tiynw.mongodb.net/?retryWrites=true&w=majority')
db = client["youtube_db"]
collection = db["youtube_demo1"]

# Connect to MySQL
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Shine@123',
    database='youtubedb1',
    auth_plugin="mysql_native_password"
)

# Create a cursor object
cursor = connection.cursor()

# SQL query to create the table
create_table_query = """
CREATE TABLE IF NOT EXISTS your_table_name (
    channel_name VARCHAR(255),
    channel_description TEXT,
    channel_id VARCHAR(255),
    subscribers INT,
    total_videos INT,
    published_at DATETIME,
    published_date DATE
);
"""

# Execute the query
cursor.execute(create_table_query)

# Commit the changes
connection.commit()

# Streamlit app
def main():
    st.title("MySQL Insert App")

    # Initialize st.session_state
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())

    # User input for channel ID
    channel_id_input = st.text_input("Enter Channel ID:", key=f"channel_id_input_{st.session_state.session_id}")

    # Button to insert data into MySQL
    if st.button("Insert into MySQL"):
        # Fetch data from MongoDB for the given channel ID
        mongo_data = collection.find_one({"Channel ID": channel_id_input})

        if mongo_data:
            # Insert data into MySQL
            insert_into_mysql(cursor, mongo_data)

        else:
            st.error("No data found for the given Channel ID in MongoDB.")

if __name__ == "__main__":
    main()

# Close MySQL connection
cursor.close()
connection.close()

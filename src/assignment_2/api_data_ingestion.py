# Databricks notebook source
import requests
import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, current_date, regexp_extract


# COMMAND ----------

base_api_url = "https://reqres.in/api/users"
all_users_data = []
page_num = 2
total_pages = None
print("Starting data fetch from API...")

# COMMAND ----------

while True:
    try:
        # Construct the API URL for the current page
        api_url = f"{base_api_url}?page={page_num}"
        print(f"Fetching page: {page_num}")

        # Make the HTTP GET request  API
        response = requests.get(api_url)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx status codes)

        # Parse the JSON response
        response_json = response.json()

        # Extract the 'data' array, which contains the list of user dictionaries
        current_page_users = response_json.get('data', [])

        # If the 'data' array is empty, it means we have fetched all available pages
        if not current_page_users:
            print(f"No more user data found on page {page_num}. Stopping API fetch.")
            break 

        # Add the users from the current page to our overall list
        all_users_data.extend(current_page_users)

        # On the first iteration, capture the total_pages from the API response
        if total_pages is None:
            total_pages = response_json.get('total_pages')

        # If we have reached or exceeded the total_pages, stop fetching
        if total_pages is not None and page_num >= total_pages:
            print(f"Fetched all {total_pages} pages as indicated by API. Stopping fetch.")
            break

        page_num += 1 # Move to the next page

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data from the API: {e}")
        break 
    except json.JSONDecodeError:
        print(f"Error decoding JSON response from page {page_num}. Response might not be valid JSON.")
        break 
    except Exception as e:
        print(f"An unexpected error occurred during API processing: {e}")
        break 

print(f"Total number of user records fetched: {len(all_users_data)}")

custom_user_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("avatar", StringType(), True)
])

# Create the Spark DataFrame from the collected user data using the custom schema
users_df = spark.createDataFrame(all_users_data, schema=custom_user_schema)

print("Initial DataFrame Schema (after creating from fetched data):")
users_df.printSchema()
print("\nFirst 5 rows of the initial DataFrame:")
users_df.display()




# COMMAND ----------

from pyspark.sql.functions import lit, current_date

df = users_df.withColumn("site_address", lit("reqres.in")) \
       .withColumn("load_date", current_date())

display(df)

     

# COMMAND ----------

df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save("/Volumes/workspace/default/data_csv/site_info/person_info")

df2 = spark.read.format("delta").load("/Volumes/workspace/default/data_csv/site_info/person_info")
display(df2)
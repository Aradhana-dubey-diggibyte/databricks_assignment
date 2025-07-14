# DataBricks assignment 

## Assignment-1
1.	Create 3 folders as source_to_bronze, bronze_to_silver, silver_to_gold.
2.	Create 4 notebooks in this respective order.
•	2 Notebooks named in source_to_bronze as utils (add all common functions in this notebook) and employee_source_to_bronze (driver notebook)
•	1 Notebook in bronze to silver as employee_bronze_to_silver 
•	1 Notebook in silver to gold as employee_silver_to_gold
3.	Read the 3 datasets as Dataframe in employee_source_to_bronze, call utils notebook in this notebook, and write to a location in DBFS,
as /source_to_bronze/file_name.csv (employee, department_df, country_df) as CSV format.
4.	In employee_bronze_to_silver, call utils notebook in this notebook.
Read the file located in DBFS location source_to_bronze with as data frame different read methods using custom schema.
5.	convert the Camel case of the columns to the snake case using UDF.
6.	Add the load_date column with the current date.
•	The primary key is EmployeeID, the Database name is Employee_info, Table name is dim_employee.
•	write the DF as a delta table to the location /silver/db_name/table_name.
7.	In gold notebook employee_silver_to_gold, call utils notebook in this notebook
 Read the table stored in a silver layer as DataFrame and select the columns based on the following requirements.
8.	Requirements:
•	Find the salary of each department in descending order.
•	Find the number of employees in each department located in each country.
•	List the department names along with their corresponding country names.
•	What is the average age of employees in each department?
•	Add the at_load_date column to data frames.
•	Write the df to dbfs location /gold/employee/table_name(fact_employee) with overwrite and replace where condition on at_load_date.

# Assignment-2

**Steps:**

1.  **API Data Fetching:**
    * Iteratively fetch data from the API endpoint `https://reqres.in/api/users` by incrementing the `page` parameter.
    * Continue fetching data until the API response for the `data` array is empty, indicating no more pages are available.
    * Combine all fetched data into a single collection.

2.  **Initial Data Preparation:**
    * From the fetched JSON response, drop the top-level keys: `"page"`, `"per_page"`, `"total"`, `"total_pages"`, and the entire `"support"` block. The focus is solely on the `data` array content.

3.  **DataFrame Creation with Custom Schema:**
    * Read the collected user data (from the `data` array) into a DataFrame.
    * Apply a custom schema during the read process to ensure correct data types and structure.

4.  **Flattening the DataFrame:**
    * Flatten the DataFrame to ensure all fields from the nested `data` array are at the top level, creating a flat table structure.

5.  **Derive `site_address` Column:**
    * Create a new column named `site_address`.
    * Populate this column by extracting the domain name (`reqres.in`) from the `email` column for each record.

6.  **Add `load_date` Column:**
    * Add a new column named `load_date` to the DataFrame.
    * Populate this column with the current date, reflecting when the data was loaded.

7.  **Write to volume (Delta Format):**
    * Write the final transformed DataFrame to a volume location.
    * The database name will be `site_info`.
    * The table name will be `person_info`.
    * The data should be written in Delta Lake format.
    * Use `overwrite` mode to replace existing data at the specified location with the new data.
    * The target volume path will be `/site_info/person_info`.




import pandas as pd
import mysql.connector
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('order_items.csv', 'order_items'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('geolocation.csv', 'geolocation'),
    ('payments.csv', 'payments')
]

# Connect to the MySQL database
try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='rootroot',
        database='ecomm'
    )
    cursor = conn.cursor()
    print("Database connection successful!")
except mysql.connector.Error as err:
    print(f"Database connection failed: {err}")
    exit()

# Folder containing the CSV files
folder_path = './archive'


# Function to map pandas data types to MySQL types
def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

# Process each CSV file
for csv_file, table_name in csv_files:
    try:
        file_path = os.path.join(folder_path, csv_file)

        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(file_path)

        # Replace NaN with None to handle SQL NULL
        df = df.where(pd.notnull(df), None)

        # Debugging: Check the DataFrame
        print(f"Processing {csv_file} - Preview:")
        print(df.head())
        print(df.info())

        # Clean column names
        df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

        # Generate the CREATE TABLE statement
        columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
        create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
        print(f"Executing: {create_table_query}")
        cursor.execute(create_table_query)

        # Batch insert data into the table
        rows = [tuple(None if pd.isna(x) else x for x in row) for row in df.to_numpy()]
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(df.columns))})"
        print(f"Inserting {len(rows)} rows into `{table_name}`")
        cursor.executemany(sql, rows)
        conn.commit()
        print(f"Successfully processed {csv_file}")

    except Exception as e:
        print(f"Error processing {csv_file}: {e}")

# Close the connection
conn.close()

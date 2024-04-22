import boto3
import csv
import psycopg2
from datetime import datetime
import pytz

# AWS Region
AWS_REGION = 'us-east-1'

# PostgreSQL connection details
PG_HOST = 'example-postgres.c3u0msuem7ho.us-east-1.rds.amazonaws.com'
PG_DATABASE = 'postgres'
PG_USER = 'rahil'
PG_PASSWORD = 'Rahil1234'
PG_PORT = '5432'

# S3 bucket and file details
BUCKET_NAME = 'finaltaskbucket2'
FILE_NAME = 'uploaded_file.csv'

# Define the Indian timezone
INDIAN_TIMEZONE = pytz.timezone('Asia/Kolkata')

def download_csv_from_s3(bucket_name, file_name):
    s3 = boto3.client('s3', region_name=AWS_REGION)
    with open('/tmp/' + file_name, 'wb') as f:
        s3.download_fileobj(bucket_name, file_name, f)

def insert_data_into_postgres(cursor, file_name):
    with open('/tmp/' + file_name, 'r') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            # Skip empty rows or rows with insufficient data
            if len(row) != 5:
                print("Skipping row:", row)
                continue

            # Get the current date and time in the Indian timezone
            current_time = datetime.now(INDIAN_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")

            # Insert data into PostgreSQL table with current date and time
            cursor.execute("""
                INSERT INTO Employee (empID, name, age, email, city, inserted_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, row + [current_time])

def lambda_handler(event, context):
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )
    cursor = conn.cursor()

    try:
        # Download CSV file from S3
        download_csv_from_s3(BUCKET_NAME, FILE_NAME)

        # Insert data from CSV into PostgreSQL table
        insert_data_into_postgres(cursor, FILE_NAME)

        # Commit changes
        conn.commit()
        print("Data inserted successfully.")

        # Display data from PostgreSQL table
        cursor.execute("SELECT * FROM Employee")
        inserted_data = cursor.fetchall()
        for row in inserted_data:
            print(row)

        # Return load success if data insertion is successful
        return "load success"

    except Exception as e:
        print("Error:", e)
        conn.rollback()

        # Return load failed if there's an error
        return "load failed"

    finally:
        # Close database connection
        cursor.close()
        conn.close()


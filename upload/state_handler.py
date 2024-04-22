import json
import boto3

s3 = boto3.client('s3')

def extract_csv_data(body):
    # Split the body by lines
    lines = body.split('\n')

    # Find the index where the CSV data starts
    start_index = None
    for index, line in enumerate(lines):
        if line.startswith("Content-Type: text/csv"):
            start_index = index + 2  # Skip the Content-Type line and an empty line
            break
    
    if start_index is None:
        return None  # CSV data not found

    # Extract CSV data from the lines
    csv_data = "\n".join(lines[start_index:]).strip()
    return csv_data

def lambda_handler(event, context):
    try:
        # Extract file data from the event
        file_content = event['body']  # Assuming the file content is directly provided

        # Extract CSV data from the file content
        csv_data = extract_csv_data(file_content)

        if not csv_data:
            raise ValueError("CSV data not found in the provided body")

        # Define S3 bucket and file name
        bucket_name = 'finaltaskbucket2'
        file_name = 'uploaded_file.csv'  # You can specify any desired file name here

        # Upload CSV data to S3 without any modifications
        s3.put_object(Body=csv_data.encode('utf-8'), Bucket=bucket_name, Key=file_name)

        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'CSV data uploaded successfully to S3'})
        }
    except Exception as e:
        # Return error response if any exception occurs
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


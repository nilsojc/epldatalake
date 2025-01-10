import boto3
import json
import time
import requests
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# AWS configurations
region = "us-east-1"  # Replace with your preferred AWS region
bucket_name = "sports-epl-data-lake"  # Change to a unique S3 bucket name
glue_database_name = "glue_epl_data_lake"
athena_output_location = f"s3://{bucket_name}/athena-results/"

# Sportsdata.io configurations (loaded from .env)
api_key = os.getenv("SPORTS_DATA_API_KEY")  # Get API key from .env
epl_endpoint = "https://premier-league-standings1.p.rapidapi.com/"

# Create AWS clients
s3_client = boto3.client("s3", region_name=region)
glue_client = boto3.client("glue", region_name=region)
athena_client = boto3.client("athena", region_name=region)


def create_s3_bucket():
    """Create an S3 bucket for storing sports data."""
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"S3 bucket '{bucket_name}' created successfully.")
    except Exception as e:
        print(f"Error creating S3 bucket: {e}")


def create_glue_database():
    """Create a Glue database for the data lake."""
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": glue_database_name,
                "Description": "Glue database for Premier League football analytics.",
            }
        )
        print(f"Glue database '{glue_database_name}' created successfully.")
    except Exception as e:
        print(f"Error creating Glue database: {e}")


def fetch_epl_data():
    """Fetch EPL standings data from the RapidAPI endpoint."""
    try:
        headers = {
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "premier-league-standings1.p.rapidapi.com",
        }
        response = requests.get(epl_endpoint, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes
        print("Fetched EPL data successfully.")
        return response.json()  # Return JSON response
    except Exception as e:
        print(f"Error fetching EPL data: {e}")
        return []


def convert_to_line_delimited_json(data):
    """Convert data to line-delimited JSON format."""
    print("Converting data to line-delimited JSON format...")
    return "\n".join([json.dumps(record) for record in data])


def upload_data_to_s3(data):
    """Upload EPL data to the S3 bucket."""
    try:
        # Convert data to line-delimited JSON
        line_delimited_data = convert_to_line_delimited_json(data)

        # Define S3 object key
        file_key = "raw-data/epl_standings_data.jsonl"

        # Upload JSON data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=line_delimited_data
        )
        print(f"Uploaded data to S3: {file_key}")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")


def create_glue_table():
    """Create a Glue table for the data."""
    try:
        glue_client.create_table(
            DatabaseName=glue_database_name,
            TableInput={
                "Name": "epl_standings",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "team", "Type": "struct<name:string,logo:string,abbreviation:string>"},
                        {
                            "Name": "stats",
                            "Type": (
                                "struct<wins:int,losses:int,ties:int,gamesPlayed:int,goalsFor:int,"
                                "goalsAgainst:int,points:int,rank:int,goalDifference:int>"
                            )
                        },
                    ],
                    "Location": f"s3://{bucket_name}/raw-data/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"
                    },
                },
                "TableType": "EXTERNAL_TABLE",
            },
        )
        print(f"Glue table 'epl_standings' created successfully.")
    except Exception as e:
        print(f"Error creating Glue table: {e}")


def configure_athena():
    """Set up Athena output location."""
    try:
        athena_client.start_query_execution(
            QueryString="CREATE DATABASE IF NOT EXISTS epl_analytics",
            QueryExecutionContext={"Database": glue_database_name},
            ResultConfiguration={"OutputLocation": athena_output_location},
        )
        print("Athena output location configured successfully.")
    except Exception as e:
        print(f"Error configuring Athena: {e}")


# Main workflow
def main():
    print("Setting up data lake for Premier League analytics...")
    create_s3_bucket()
    time.sleep(5)  # Ensure bucket creation propagates
    create_glue_database()
    epl_data = fetch_epl_data()
    if epl_data:  # Only proceed if data was fetched successfully
        upload_data_to_s3(epl_data)
    create_glue_table()
    configure_athena()
    print("Data lake setup complete.")


if __name__ == "__main__":
    main()

import boto3
import json
import time
import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# AWS configurations
region = "us-east-1"  # Replace with your preferred AWS region
bucket_name = "sports-epl-data-lake"  # Change to a unique S3 bucket name
glue_database_name = "glue_epl_data_lake"
athena_output_location = f"s3://{bucket_name}/athena-results/"

# API-Football configurations
api_key = os.getenv("SPORTS_DATA_API_KEY")  # Load API key from .env file
if not api_key:
    raise ValueError("API key not found. Make sure SPORTS_DATA_API_KEY is set in your .env file.")
api_host = "v3.football.api-sports.io"

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

def fetch_epl_standings(league_id, season):
    """Fetch EPL standings data using requests."""
    try:
        url = f"https://{api_host}/standings"
        params = {
            "league": 39,
            "season": 2023,
        }
        headers = {
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": api_host,
        }
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Error fetching data: {response.status_code} {response.text}")
            return None

        response_json = response.json()
        if not response_json.get("response"):
            print("No data found in API response.")
            return None

        print("Standings data fetched successfully.")
        return response_json["response"]

    except Exception as e:
        print(f"Exception occurred: {e}")
        return None

def extract_standings(data):
    """Extract standings information from the API response."""
    try:
        if not data:
            print("No data available to extract.")
            return []

        league_data = data[0].get("league", {})
        standings = league_data.get("standings", [])
        if not standings or not isinstance(standings[0], list):
            print("Standings data is missing or malformed in API response.")
            return []

        print("Standings data extracted successfully.")
        return standings[0]  # Return the first group of standings
    except Exception as e:
        print(f"Error extracting standings data: {e}")
        return []

def convert_to_line_delimited_json(data):
    """Convert data to line-delimited JSON format."""
    print("Converting data to line-delimited JSON format...")
    return "\n".join([json.dumps(record) for record in data])

def upload_data_to_s3(data):
    """Upload EPL data to the S3 bucket."""
    try:
        line_delimited_data = convert_to_line_delimited_json(data)
        file_key = "raw-data/epl_standings_data.jsonl"
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
                        {"Name": "rank", "Type": "int"},
                        {"Name": "team", "Type": "struct<id:int,name:string,logo:string>"},
                        {"Name": "points", "Type": "int"},
                        {"Name": "goalsDiff", "Type": "int"},
                        {"Name": "group", "Type": "string"},
                        {"Name": "form", "Type": "string"},
                        {"Name": "status", "Type": "string"},
                        {"Name": "description", "Type": "string"},
                        {"Name": "all", "Type": "struct<played:int,win:int,draw:int,lose:int,goals:struct<for:int,against:int>>"},
                        {"Name": "home", "Type": "struct<played:int,win:int,draw:int,lose:int,goals:struct<for:int,against:int>>"},
                        {"Name": "away", "Type": "struct<played:int,win:int,draw:int,lose:int,goals:struct<for:int,against:int>>"},
                        {"Name": "update", "Type": "string"},
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

    # Fetch and process EPL standings data
    epl_data = fetch_epl_standings(league_id=39, season=2023)
    standings = extract_standings(epl_data)
    if standings:
        upload_data_to_s3(standings)

    create_glue_table()
    configure_athena()
    print("Data lake setup complete.")

if __name__ == "__main__":
    main()

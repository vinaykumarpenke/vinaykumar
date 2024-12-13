import json
import pandas as pd
import boto3
import concurrent.futures
from datetime import datetime, date
import os
from ma_ds_multi_api_layer import format_logging_msg, create_connection_pool, get_connection, release_connection, logger

# Importing Watchtower for CloudWatch logging
import watchtower, logging

# CloudWatch logging setup
CUSTOM_LOG_GROUP = f"/aws/lambda/{os.getenv('AWS_LAMBDA_FUNCTION_NAME')}"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.addHandler(watchtower.CloudWatchLogHandler(log_group=CUSTOM_LOG_GROUP))

# Helper function to serialize datetime or date objects to string format
def serialize_data(result):
    try:
        log_msg = format_logging_msg(source="file_status_api", message="Serializing data result", definition_nm="serialize_data")
        logger.info(log_msg)
        for row in result:
            for key, value in row.items():
                if isinstance(value, (datetime, date)):
                    row[key] = value.isoformat()  # Convert date/datetime to string
        return result
    except Exception as e:
        log_msg = format_logging_msg(source="file_status_api", message=f"Error occurred serializing data: {str(e)}", definition_nm="serialize_data")
        logger.error(log_msg, exc_info=True)

# Dynamically built SQL query based on filter parameters
def build_sql_query(filter, accordion_view_api_config):
    try:
        log_msg = format_logging_msg(source="file_status_api", message=f"Building SQL query with filter: {filter}", definition_nm="build_sql_query")
        logger.info(log_msg)

        base_query = accordion_view_api_config["base_query"]
        conditions = []

        # Adding filters based on the request
        for key, value in filter.items():
            if isinstance(value, list):
                conditions.append(f"{key} IN ({','.join(map(str, value))})")
            else:
                conditions.append(f"{key} = '{value}'")

        query = base_query + " WHERE " + " AND ".join(conditions) + " LIMIT {limit} OFFSET {offset};"
        log_msg = format_logging_msg(source="file_status_api", message=f"SQL Query successfully built: {query}", definition_nm="build_sql_query")
        logger.info(log_msg)
        return query
    except Exception as e:
        log_msg = format_logging_msg(source="file_status_api", message=f"Error occurred building SQL query: {str(e)}", definition_nm="build_sql_query")
        logger.error(log_msg, exc_info=True)

# Fetch data from the database based on the SQL query
def fetch_data(query):
    try:
        log_msg = format_logging_msg(source="file_status_api", message=f"Fetching data for query: {query}", definition_nm="fetch_data")
        logger.info(log_msg)
        connection_pool = create_connection_pool("file_status_api")
        connection = get_connection(connection_pool, "file_status_api")
        df = pd.read_sql(query, connection)
        release_connection(connection_pool, connection, "file_status_api")
        return df
    except Exception as e:
        log_msg = format_logging_msg(source="file_status_api", message=f"Error occurred fetching data for query: {str(e)}", definition_nm="fetch_data")
        logger.error(log_msg, exc_info=True)
        return {
            'statusCode': 500,
            'message': f"Error: {str(e)}",
            'body': None
        }

# Main Lambda handler function that processes events
def lambda_handler(event, context):
    try:
        log_msg = format_logging_msg(source="file_status_api", message=f"Lambda handler started with event: {event}", definition_nm="lambda_handler")
        logger.info(log_msg)

        # Extracting parameters from the event
        user_email = event["user_email"]
        file_status = event["file_status"]
        page = event.get("page", 1)
        limit = event.get("limit", 10)

        # Validate required fields
        if not user_email or not file_status:
            error_message = "Bad Request: Missing required parameters (user_email or file_status)"
            log_msg = format_logging_msg(source="file_status_api", message=error_message, definition_nm="lambda_handler")
            logger.error(log_msg, exc_info=True)
            return {
                "statusCode": 400,
                "headers": {'Content-Type': 'application/json'},
                "body": error_message
            }

        # Define the offset for pagination
        offset = (page - 1) * limit

        # Fetch config file key from environment variable
        config_file_key = os.getenv('config_file_path')
        logger.info(f"Using config file path: {config_file_key}")

        # Fetch config from S3
        s3_client = boto3.client("s3")
        S3_BUCKET_CONFIG = os.getenv('config_bucket')
        config_object = s3_client.get_object(Bucket=S3_BUCKET_CONFIG, Key=config_file_key)
        config_body = config_object['Body'].read().decode('utf-8')
        dict_config = dict(json.loads(config_body))
        accordion_view_api_config = dict_config['accordion_view_api']

        # Build and execute query
        filter_params = {
            "user_email": user_email,
            "file_status": file_status
        }
        
        query = build_sql_query(filter_params, accordion_view_api_config).format(limit=limit, offset=offset)
        
        # Execute the query concurrently (in case of multiple queries)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(fetch_data, [query]))
        
        # Combine results into a single DataFrame
        combined_df = pd.concat(results, ignore_index=True)
        result = combined_df.to_dict(orient="records")
        result = serialize_data(result)

        log_msg = format_logging_msg(source="file_status_api", message=f"Data processing successful, returning {len(result)} records", definition_nm="lambda_handler")
        logger.info(log_msg)

        if result:
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps(result)
            }
        else:
            error_message = "No data found for the given query"
            log_msg = format_logging_msg(source="file_status_api", message=error_message, definition_nm="lambda_handler")
            logger.error(log_msg, exc_info=True)
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'message': error_message})
            }
    except Exception as e:
        log_msg = format_logging_msg(source="file_status_api", message=f"Error occurred in lambda_handler: {str(e)}", definition_nm="lambda_handler")
        logger.error(log_msg, exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)})
        }

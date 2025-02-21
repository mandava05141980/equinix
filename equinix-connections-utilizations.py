import os
import json
import requests
import re
from dotenv import load_dotenv
from influxdb import InfluxDBClient
from datetime import datetime, timedelta

# Load .env file
load_dotenv()
campus_client = InfluxDBClient(host=os.getenv('INFLUXDB_HOST'), port=8086, username=os.getenv('CAMPUS_DB_USER'), password=os.getenv('CAMPUS_DB_PASS'), database=os.getenv('CAMPUS_DB'),ssl=True,verify_ssl=True)
network_client = InfluxDBClient(host=os.getenv('INFLUXDB_HOST'), port=8086, username=os.getenv('NETWORK_DB_USER'), password=os.getenv('NETWORK_DB_PASS'), database=os.getenv('INFLUXDB_DB'),ssl=True,verify_ssl=True)
#local_client = InfluxDBClient('localhost', 8086, 'local_db', 'local123', 'call_quality')

# List of UUIDs to exclude from the metrics
EXCLUDED_UUIDS = [
    "8d35a896-f363-4ff6-8d07-470d398cbdf5",
    "e6fc49c5-c5b2-4ed0-acb6-7ac915b175b0",
    "33149ec5-943d-418a-9a22-b99bad5d1e11",
    "9f917a9e-24e3-4353-8db4-55c708d15243"
]

# Function 1: Get OAuth token
def get_oauth_token():
    url = "https://api.equinix.com/oauth2/v1/token"
    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")
    if not client_id or not client_secret:
        raise ValueError("client_id or client_secret not found in .env file")
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url, data=payload, headers=headers)
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise ValueError("Failed to retrieve access token")
    return token

# Function 2: Fetch all connections using the /connections/search endpoint (POST request)
def fetch_all_connections(token):
    url = "https://api.equinix.com/fabric/v4/connections/search"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    search_payload = {
        "filter": {
            "and": [
                {
                    "property": "/direction",
                    "operator": "=",
                    "values": [
                        "OUTGOING",
                        "INTERNAL"
                    ]
                }
            ]
        },
        "pagination": {
            "limit": 100,
            "offset": 0
        },
        "sort": [
            {
                "property": "/changeLog/updatedDateTime",
                "direction": "DESC"
            }
        ]
    }
    response = requests.post(url, headers=headers, json=search_payload)
    response.raise_for_status()
    search_results = response.json().get("data", [])
    if not search_results:
        raise ValueError("No search results found")
    return search_results

# Function 3: Fetch connection stats
def fetch_connection_stats(token, connection_id, start_time, end_time):
    url = f"https://api.equinix.com/fabric/v4/connections/{connection_id}/stats"
    params = {
        "startDateTime": start_time,
        "endDateTime": end_time,
        "viewPoint": "aSide"
    }
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    stats = response.json().get("stats", {}).get("bandwidthUtilization", {})
    inbound = stats.get("inbound", {})
    outbound = stats.get("outbound", {})

    # Convert scientific notation to normal float values
    def convert_scientific_to_float(value, decimals=6):
        try:
            return "{:.{decimals}f}".format(float(value), decimals=decimals)
        except (ValueError, TypeError):
            return value

    return {
        "inbound": {
            "max": inbound.get("max"),
            "mean": inbound.get("mean"),
            "lastPolled": inbound.get("lastPolled"),
        },
        "outbound": {
            "max": outbound.get("max"),
            "mean": outbound.get("mean"),
            "lastPolled": outbound.get("lastPolled"),
        }
    }

# Function to get current timestamp in the required format
def get_current_timestamp():
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

# Function to get timestamp for 5 minutes ago
def get_past_5_minutes_timestamp():
    now = datetime.utcnow()
    past_time = now - timedelta(minutes=1440)
    return past_time.strftime('%Y-%m-%dT%H:%M:%SZ')

# Function to prepare the data for InfluxDB
def prepare_influxdb_data(connection_name, stats):
    # Prepare the measurement name from the connection name
    measurement = "equinix_multicloud_stats"
    # Prepare the tags and fields
    tags = {
        "Connection_name": connection_name
    }
    fields = {
        "max_inbound": round(stats["inbound"]["max"], 3),
        "mean_inbound": round(stats["inbound"]["mean"], 3),
        "lastPolled_inbound": round(stats["inbound"]["lastPolled"], 3),
        "max_outbound": round(stats["outbound"]["max"], 3),
        "mean_outbound": round(stats["outbound"]["mean"], 3),
        "lastPolled_outbound": round(stats["outbound"]["lastPolled"], 3)
    }
    # Creating a data point to send to InfluxDB
    json_body = [{
        "measurement": measurement,
        "tags": tags,
        "fields": fields
    }]
    return json_body

# Function to print the measurement in the desired format
def print_influxdb_data(influx_data):
    # Extract the first entry from the influx_data list
    data = influx_data[0]
    # Extract measurement, tags, and fields
    measurement = data['measurement']
    tags = data['tags']
    fields = data['fields']
    # Prepare the output in the desired format (it should be wrapped in a list)
    output = [{
        "measurement": measurement,
        "tags": tags,
        "fields": fields
    }]
    # Convert the dictionary to a JSON string without spaces between key-value pairs
    json_str = json.dumps(output, separators=(',', ':'))  # No extra spaces
    # Replace quotes around numbers (and around the dictionary keys)
    json_str = json_str.replace('"', "'")
    # Remove quotes around numeric values (i.e., float values should not be quoted)
    json_str = re.sub(r"'(\d+\.\d+)'", r"\1", json_str)
    # Print the result without the extra double brackets
    print(json_str)

# Main logic
def main():
    try:
        # Get OAuth token
        token = get_oauth_token()
        # Fetch all connections from Equinix API using POST /connections/search
        connections = fetch_all_connections(token)
        # Generate timestamps for the query
        end_time = get_current_timestamp()
        start_time = get_past_5_minutes_timestamp()
        # Iterate through connections and fetch stats
        for connection in connections:
            connection_id = connection.get("uuid")  # Fetch uuid from search results
            connection_name = connection.get("name")  # Fetch name from search results
            connection_state = connection.get("state")  # Fetch state from search results
            # Skip connection if its UUID is in the excluded list
            if connection_id in EXCLUDED_UUIDS:
            #    print(f"Skipping excluded connection with UUID: {connection_id}")
                continue
            # Process only if the connection state is "ACTIVE"
            if connection_id and connection_name and connection_state == "ACTIVE":
                stats = fetch_connection_stats(token, connection_id, start_time, end_time)
                # Prepare the data for InfluxDB
                influx_data = prepare_influxdb_data(connection_name, stats)
                campus_client.write_points(influx_data)
                network_client.write_points(influx_data)
                # local_client.write_points(influx_data)
                # Print the measurement in the desired format
                print_influxdb_data(influx_data)
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
    except ValueError as e:
        print(f"Value error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()


from datetime import datetime, timedelta
from collections import defaultdict
from loguru import logger
import mysql.connector                      # Import MySQL connector for database operations
import schedule
import requests
import time
import threading
import json
import sys
import os

# ----------------------- Configuration Values -----------------------
Program_Name = "TARA-Xilog-Server"                  # Program name for identification and logging.
Program_Version = "1.1"                             # Program version used for file naming and logging.
# ---------------------------------------------------------------------

# Default configuration dictionary for the application
default_config = {
    "DB_config": {                           # MySQL configuration
        "mysql_host": "",                    # MySQL host address
        "mysql_port": 3306,                  # Default MySQL port
        "mysql_user": "",                    # MySQL username
        "mysql_Pass": "",                    # MySQL password
        "mysql_database": "",                # MySQL database name
    },
    "URL": "",                               # Default URL for HTTP GET requests.
    "Sensor": [
        {
            "Token": "",
            "Sensor_ID": "",
            "Name": "Default Sensor",
            "Get_Type": "day",
            "Get_Value": 10
        }
    ],                                       # Default Sensor ID for HTTP GET requests.
    "TimeSleep": 8,                          # Default sleep time in seconds between HTTP GET requests.
    "SleepType": "seconds",                  # Default sleep type for scheduling.
    # Options: "seconds", "minutes", "hours", "days"
    "log_Level": "DEBUG",
    "Log_Console_Enable": 1,                 # Set to "true" to enable console logging.
    "log_Backup": 90,                        # Log retention duration (number of backup files).
    "Log_Size": "10 MB"                      # Maximum log file size before rotation.
}

def Load_Config(default_config, Program_Name):
    """
    Load configuration from JSON file. If the file does not exist, create it with default values.
    """
    config_file_path = f'{Program_Name}.config.json'

    # Create config file with default values if it does not exist.
    if not os.path.exists(config_file_path):
        default_config = default_config 
        with open(config_file_path, 'w') as new_config_file:
            json.dump(default_config, new_config_file, indent=4)

    # Load configuration from file
    with open(config_file_path, 'r') as config_file:
        config = json.load(config_file)
    
    return config

# ----------------------- Loguru Logging Setup -----------------------
def Loguru_Logging(config, Program_Name, Program_Version):
    """
    Set up Loguru logging based on configuration.
    """
    logger.remove()

    log_Backup = int(config.get('log_Backup', 90))  # Default to 90 days if not set
    if log_Backup < 1:  # Ensure log retention is at least 1 day
        log_Backup = 1
    Log_Size = config.get('Log_Size', '10 MB')  # Default to 10 MB if not set
    log_Level = config.get('log_Level', 'DEBUG').upper()  # Default to DEBUG if not set

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_file_name = f'{Program_Name}_{Program_Version}.log'
    log_file = os.path.join(log_dir, log_file_name)

    # Enable console logging if configured
    if config.get('Log_Console_Enable', 0) == 1:
        logger.add(
            sys.stdout, 
            level=log_Level, 
            format="<green>{time}</green> | <blue>{level}</blue> | <cyan>{thread.id}</cyan> | <magenta>{function}</magenta> | {message}"
        )

    # Add file logging with rotation and retention
    logger.add(
        log_file,
        format="{time} | {level} | {thread.id} | {function} | {message}",
        level=log_Level,
        rotation=Log_Size,
        retention=log_Backup,
        compression="zip"
    )

    logger.info('-' * 117)
    logger.info(f"Start {Program_Name} Version {Program_Version}")
    logger.info('-' * 117)

    return logger

# ----------------------- HTTP GET Example -----------------------

def fetch_sensor(sensor, config):
    """
    Fetch data for a single sensor from the configured URL and process the response.
    """
    logger.info("Processing Sensor ID: {}", sensor['Sensor_ID'])
    # Determine the start datetime based on Get_Type and Get_Value
    if 'Get_Type' in sensor and sensor['Get_Type'].lower() == 'day':
        # If Get_Type is 'day', fetch data for the last 'Get_Value' days, but not more than 8 days
        get_value = sensor.get('Get_Value', 1)
        if get_value > 8:
            logger.warning("Get_Value {} is greater than 8, limiting to 8 days.", get_value)
            get_value = 8
        StartDatetime = (datetime.now() - timedelta(days=get_value)).strftime("%Y-%m-%d %H:%M")
    elif 'Get_Type' in sensor and sensor['Get_Type'].lower() == 'hour':
        # If Get_Type is 'hour', fetch data for the last 'Get_Value' hours
        StartDatetime = (datetime.now() - timedelta(hours=sensor.get('Get_Value', 1))).strftime("%Y-%m-%d %H:%M")
    elif 'Get_Type' in sensor and sensor['Get_Type'].lower() == 'minute':
        # If Get_Type is 'minute', fetch data for the last 'Get_Value' minutes
        StartDatetime = (datetime.now() - timedelta(minutes=sensor.get('Get_Value', 1))).strftime("%Y-%m-%d %H:%M")
    else:
        # Default to fetching data for the last day if no valid Get_Type is specified
        StartDatetime = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M")
    # EndDatetime is set to now
    EndDatetime = datetime.now().strftime("%Y-%m-%d %H:%M")
    logger.debug("StartDatetime: {}, EndDatetime: {}", StartDatetime, EndDatetime)
    # Construct the URL for the HTTP GET request
    base_url = config.get('URL', 'https://localhost/')
    if base_url.endswith('/'):
        base_url = base_url[:-1]
    url = f"{base_url}/{sensor['Sensor_ID']}/{StartDatetime.replace(' ', '%20')}/{EndDatetime.replace(' ', '%20')}/{sensor.get('Token')}"
    logger.debug("Fetching data from URL: {}", url)
    # Perform the HTTP GET request
    logger.info("Fetching data for Sensor ID: {} from URL: {}", sensor['Sensor_ID'], url)
    try:
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()
        if not isinstance(json_data, list):
            logger.error("Invalid response format: Expected a list, got {}", type(json_data).__name__)
            return
        combined_data = defaultdict(dict)

        # Check if the device ID exists in the database
        if(CheckDeviceId(sensor['Sensor_ID']) == 1):
            logger.info("Sensor ID {} already exists in the database, insert into database.", sensor['Sensor_ID'])
            logger.info("success: {}", response.text)
            # Combine data by timestamp and channel
            for item in json_data:
                unit = item["unit"]
                channel = item["channelName"]
                for data_entry in item["data"]:
                    if data_entry["type"] == 8:
                        for d in data_entry["data"]:
                            ts = d["timestamp"]
                            combined_data[ts][channel] = d["value"]

            # Insert each timestamp's data into the database
            for ts in sorted(combined_data.keys()):
                flow = combined_data[ts].get("Flow1", "-")
                pressure = combined_data[ts].get("Pressure1", "-")
                if flow == "-" or pressure == "-":
                    logger.warning("Missing data for timestamp {}: Flow={}, Pressure={}", ts, flow, pressure)
                    continue
                logger.debug("Inserting data for timestamp {}: Flow={}, Pressure={}", ts, flow, pressure)
                try:
                    InsertDB(
                        DeviceID=sensor['Sensor_ID'],
                        DateTime=ts,
                        MeterIndex=0, 
                        Pressure=pressure,
                        Flowrate=flow,
                        FrameType="Xilog"
                    )
                except mysql.connector.Error as e:
                    logger.error("Database insert error for Sensor ID {}: {}", sensor['Sensor_ID'], str(e))
            logger.info("Data for Sensor ID {} processed successfully.", sensor['Sensor_ID'])
        else:
            logger.debug("Sensor ID {} does not exist in the database, skipping insert.", sensor['Sensor_ID'])

    except requests.RequestException as e:
        logger.error("failed: {}", str(e))

def CheckDeviceId(DeviceID):
    """
    Check if the given DeviceID exists in the database.
    Returns 1 if exists, 0 otherwise.
    """
    logger.info('Database Check Device ID Start')
    
    DB_config = config.get("DB_config", {})
    if not DB_config:
        logger.error("Database configuration not found in config.json.")
        return 0 # Return 0 if no config found
    host = DB_config.get("mysql_host", "localhost")
    port = DB_config.get("mysql_port", 3306)  # Default MySQL port is 3306
    user = DB_config.get("mysql_user", "root")
    password = DB_config.get("mysql_Pass", "")
    database = DB_config.get("mysql_database", "dev")

    logger.debug(f'Connect to db {database} at {host}:{port} with user {user}')

    try:
        mysql_conn = mysql.connector.connect(
            host=host,
            user=user,
            port=port,
            password=password,
            database=database
        )
        mysql_Cursor = mysql_conn.cursor(dictionary=True)
        
        QueryCheckSensorId = f"SELECT COUNT(*) AS count FROM device WHERE device_ref_id = %s"
        mysql_Cursor.execute(QueryCheckSensorId, (DeviceID,))
        result = mysql_Cursor.fetchone()  # Fetch result

        logger.debug(f'Device Id : {DeviceID} Count = {result["count"]}')
    except mysql.connector.Error as e:
        logger.exception(f"MySQL error: {e}")
    finally:
        if mysql_Cursor:
            mysql_Cursor.close()
        if mysql_conn:
            mysql_conn.close()
    return result["count"] if result else 0

def InsertDB(DeviceID, DateTime, MeterIndex, Pressure, Flowrate, FrameType):
    """
    Insert sensor data into the standarddb table in the database.
    """
    logger.debug('Database Insert Device ID {} DateTime {} MeterIndex {} Pressure {} Flowrate {} FrameType {}'
                ,DeviceID, DateTime, MeterIndex, Pressure, Flowrate, FrameType)
    
    DB_config = config.get("DB_config", {})
    if not DB_config:
        logger.error("Database configuration not found in config.json.")
        return
    host = DB_config.get("mysql_host", "localhost")
    port = DB_config.get("mysql_port", 3306)  # Default MySQL port is 3306
    user = DB_config.get("mysql_user", "root")
    password = DB_config.get("mysql_Pass", "")
    database = DB_config.get("mysql_database", "dev")

    mysql_conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    mysql_Cursor = mysql_conn.cursor()

    # Insert data with calculation for Consumption using subquery
    QueryInsertData = """
    INSERT INTO standarddb (`DeviceID`, `DateTime`, `MeterIndex`, `Pressure`, `Flowrate`, `Consumption`, `FrameType`, `updated_at`)
    VALUES (
        %s, %s, %s, %s, %s,
        %s - COALESCE((
            SELECT IFNULL(s.MeterIndex,0)
            FROM standarddb s
            WHERE s.DeviceID = %s
            ORDER BY `DateTime` DESC
            LIMIT 1
        ), 0),
        %s, NOW()
    )
    """
    # Note: MeterIndex and DeviceID are sent again for use in the subquery
    mysql_Cursor.execute(QueryInsertData, (
        DeviceID, DateTime, MeterIndex, Pressure, Flowrate,
        MeterIndex, DeviceID, FrameType
    ))
    mysql_conn.commit()

    if mysql_Cursor:
        mysql_Cursor.close()
    if mysql_conn:
        mysql_conn.close()

def get_sensor_data(config):
    """
    Create threads for each sensor and fetch their data concurrently.
    """
    sensors = config.get('Sensor', [])
    threads = []
    for sensor in sensors:
        t = threading.Thread(target=fetch_sensor, args=(sensor, config))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

def keep_alive():
    """
    Function to keep the script running and log a keep-alive message.
    """
    logger.info("Keep-alive Waiting for next scheduled task....")

# ----------------------------------------------------------------
if __name__ == "__main__":
    try:
        # Load configuration and set up logging
        config = Load_Config(default_config, Program_Name)  
        logger = Loguru_Logging(config, Program_Name, Program_Version)
        logger.info("Configuration loaded successfully.")
    except Exception as e:  
        logger.error("Error loading configuration: {}", str(e))
        sys.exit(1)
    
    # Schedule the get_sensor_data function based on SleepType
    if config.get('SleepType').upper() == "SECONDS":
        schedule.every(config.get('TimeSleep', 10)).seconds.do(get_sensor_data, config)
    elif config.get('SleepType').upper() == "MINUTES":
        schedule.every(config.get('TimeSleep', 10)).minutes.do(get_sensor_data, config)
    elif config.get('SleepType').upper() == "HOURS":
        schedule.every(config.get('TimeSleep', 10)).hours.do(get_sensor_data, config)
    elif config.get('SleepType').upper() == "DAYS":
        schedule.every(config.get('TimeSleep', 10)).days.do(get_sensor_data, config)
    else:
        logger.error("Invalid SleepType in configuration: {}", config.get('SleepType'))
        exit(1)

    get_sensor_data(config)  # Initial call to fetch data immediately

    schedule.every(1).minutes.do(keep_alive)  # Keep-alive task to run every minute

    # Main loop to run scheduled tasks
    while True:
        schedule.run_pending()
        time.sleep(10)


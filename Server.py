from datetime import datetime, timedelta
from loguru import logger
import schedule
import requests
import time
import threading
import json
import sys
import os

# ----------------------- Configuration Values -----------------------
Program_Name = "TARA-Xilog-Server"                  # Program name for identification and logging.
Program_Version = "1.0"                             # Program version used for file naming and logging.
# ---------------------------------------------------------------------

default_config = {
            "URL": "",  # Default URL for HTTP GET requests.
            "Sensor": [
                {
                    "Token": "",
                    "Sensor_ID": "",
                    "Name": "Default Sensor",
                    "Get_Type": "day",
                    "Get_Value": 10
                }
            ],  # Default Sensor ID for HTTP GET requests.
            "TimeSleep": 8,  # Default sleep time in seconds between HTTP GET requests.
            "SleepType": "seconds",  # Default sleep type for scheduling.
            # Options: "seconds", "minutes", "hours", "days"
            "log_Level": "DEBUG",
            "Log_Console": 1,         # Set to "true" to enable console logging.
            "log_Backup": 90,         # Log retention duration (number of backup files).
            "Log_Size": "10 MB"       # Maximum log file size before rotation.
        }

def Load_Config(default_config,Program_Name):
    # Define the configuration file path.
    config_file_path = f'{Program_Name}.config.json'

    # Create config file with default values if it does not exist.
    if not os.path.exists(config_file_path):
        default_config = default_config 
        with open(config_file_path, 'w') as new_config_file:
            json.dump(default_config, new_config_file, indent=4)

    # Load configuration
    with open(config_file_path, 'r') as config_file:
        config = json.load(config_file)
    
    return config

# ----------------------- Loguru Logging Setup -----------------------
def Loguru_Logging(config,Program_Name,Program_Version):
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

    if config.get('Log_Console_Enable',0) == 1:
        logger.add(
            sys.stdout, 
            level=log_Level, 
            format="<green>{time}</green> | <blue>{level}</blue> | <cyan>{thread.id}</cyan> | <magenta>{function}</magenta> | {message}"
        )

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

config = Load_Config(default_config, Program_Name)

# ----------------------- HTTP GET Example -----------------------

# ฟังก์ชันสำหรับดึงข้อมูลแต่ละ Sensor
def fetch_sensor(sensor, config):
    logger.info("Processing Sensor ID: {}", sensor['Sensor_ID'])
    if 'Get_Type' in sensor and sensor['Get_Type'].lower() == 'day':
        # If Get_Type is 'day', we fetch data for the last 'Get_Value' days, but not more than 8 days
        get_value = sensor.get('Get_Value', 1)
        if get_value > 8:
            logger.warning("Get_Value {} is greater than 8, limiting to 8 days.", get_value)
            get_value = 8
        StartDatetime = (datetime.now() - timedelta(days=get_value)).strftime("%Y-%m-%d %H:%M")
    elif 'Get_Type' in sensor and sensor['Get_Type'].lower() == 'hour':
        # If Get_Type is 'hour', we fetch data for the last 'Get_Value' hours
        StartDatetime = (datetime.now() - timedelta(hours=sensor.get('Get_Value', 1))).strftime("%Y-%m-%d %H:%M")
    elif 'Get_Type' in sensor and sensor['Get_Type'].lower() == 'minute':
        # If Get_Type is 'minute', we fetch data for the last 'Get_Value' minutes
        StartDatetime = (datetime.now() - timedelta(minutes=sensor.get('Get_Value', 1))).strftime("%Y-%m-%d %H:%M")
    else:
        # Default to fetching data for the last day if no valid Get_Type is specified
        StartDatetime = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M")
    # EndDatetime is set to now
    EndDatetime = datetime.now().strftime("%Y-%m-%d %H:%M")
    logger.debug("StartDatetime: {}, EndDatetime: {}", StartDatetime, EndDatetime)
    # Construct the URL for the HTTP GET request (Data/All/Sensor_ID/StartDatetime/EndDatetime/Token)
    base_url = config.get('URL', 'https://localhost/')
    if base_url.endswith('/'):
        base_url = base_url[:-1]
    url = f"{base_url}/{sensor['Sensor_ID']}/{StartDatetime.replace(' ', '%20')}/{EndDatetime.replace(' ', '%20')}/{sensor.get('Token')}"
    logger.debug("Fetching data from URL: {}", url)
    # Perform the HTTP GET request
    logger.info("Fetching data for Sensor ID: {} from URL: {}", sensor['Sensor_ID'], url)
     # Make the HTTP GET request
     # If the request fails, log the error
    try:
        response = requests.get(url)
        response.raise_for_status()
        logger.info("success: {}", response.text)
    except requests.RequestException as e:
        logger.error("failed: {}", str(e))

def insert_data_to_db(sensor, data):
    # Placeholder function to insert data into the database
    # This function should be implemented based on your database schema and requirements
    logger.info("Inserting data for Sensor ID: {} into the database and Data: {}", sensor['Sensor_ID'], data)
    # Example: db.insert(sensor['Sensor_ID'], data)
    pass

# ฟังก์ชันสำหรับสร้าง Thread ตามจำนวน Sensor
def get_sensor_data(config):
    sensors = config.get('Sensor', [])
    threads = []
    for sensor in sensors:
        t = threading.Thread(target=fetch_sensor, args=(sensor, config))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

# ----------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Server is running...")
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

    while True:
        schedule.run_pending()
        time.sleep(10)
        logger.debug("Waiting for next scheduled task...")
# ----------------------------------------------------------------
# Note: The above code is a simplified example and may require additional error handling and configuration management
# depending on the specific requirements of your application.   

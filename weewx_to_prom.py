import csv
import time
import sys
import logging
from datetime import datetime
from prometheus_client import start_http_server, Gauge

# Configure logging to display messages of level DEBUG and higher
logging.basicConfig(
    level=logging.DEBUG,  # Set the threshold for the logger
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the format of the log messages
    #handlers=[logging.StreamHandler(sys.stdout)]   # Only write to stdout
    handlers=[logging.FileHandler("/var/log/syslog")]  # Only write to syslog
)

# Sensor names for mapping
SENSOR_NAMES = ["hallway", "outside", "server", "bathroom", "kitchen", "children"]

# Define Prometheus Gauges with unique names for each sensor
temp_gauges = [Gauge(f'temperature_celsius_{name}', f'Temperature sensor in Celsius ({name})', ['sensor_name']) for name in SENSOR_NAMES]
humidity_gauges = [Gauge(f'humidity_percent_{name}', f'Humidity sensor in percent ({name})', ['sensor_name']) for name in SENSOR_NAMES]
data_age_gauge = Gauge('data_age_seconds', 'Age of the data in seconds')

# Default temp readings are in F, needs to be converted to C
def fahrenheit_to_celsius(fahrenheit):
    try:
        return round((fahrenheit - 32) * 5.0 / 9.0, 1)  # Modified to round temperature to 1 decimal place
    except TypeError:
        logging.error(f"Invalid type for temperature conversion: {fahrenheit}")
        return float('nan')

def process_csv(file_path):
    try:
        #logging.debug(f"Attempting to read the CSV file: {file_path}")
        with open(file_path, 'r', newline='') as csvfile:
            #logging.debug("CSV file opened successfully.")
            # Read the csv
            reader = csv.DictReader(csvfile)
            # Remove # from labels
            reader.fieldnames = [name.lstrip('#').strip() for name in reader.fieldnames]
            #logging.debug(f"Adjusted fieldnames: {reader.fieldnames}")
            rows = list(reader)  # Extract rows into a list for debugging
            #logging.debug(f"Extracted rows: {rows}")  # Added logging for debugging row extraction

            for row in rows:
                #logging.debug(f"Processing row: {row}")
                # CSV dateTime is in Unix Epoch, extract and calculate delta against current time
                try:
                    record_time = int(row['dateTime'])
                    current_time = int(time.time())
                    data_age = current_time - record_time
                    data_age_gauge.set(data_age)
                except ValueError:
                    logging.error("Invalid dateTime value.")
                    continue

                # If CSV dataTime is NOT fresh, disregard the values and export NaN
                if abs(data_age) > 60:
                    logging.warning("Data is older than 1 minute; marking as stale.")
                    for i, sensor_name in enumerate(SENSOR_NAMES): 
                        temp_gauges[i].labels(sensor_name=sensor_name).set(float('nan'))
                        humidity_gauges[i].labels(sensor_name=sensor_name).set(float('nan'))
                    continue

                # Process temperature and humidity values
                for i, sensor_name in enumerate(SENSOR_NAMES):
                    temp_key = f'temp{i}'
                    humidity_key = f'humidity{i}'

                    # Process temperature
                    temp_value = row.get(temp_key)
                    if temp_value and temp_value.lower() != 'none':
                        try:
                            temp_f = float(temp_value)
                            temp_c = fahrenheit_to_celsius(temp_f)
                            temp_gauges[i].labels(sensor_name=sensor_name).set(temp_c)
                            #logging.debug(f"Set temperature for {sensor_name} to {temp_c}")
                        except ValueError:
                            logging.error(f"Invalid temperature value for {temp_key}: {temp_value}")
                            temp_gauges[i].labels(sensor_name=sensor_name).set(float('nan'))
                    else:
                        logging.warning(f"Missing or 'None' value for {temp_key}")
                        temp_gauges[i].labels(sensor_name=sensor_name).set(float('nan'))

                    # Process humidity
                    humidity_value = row.get(humidity_key)
                    if humidity_value and humidity_value.lower() != 'none':
                        try:
                            humidity = int(float(humidity_value))  # Modified to convert humidity to integer
                            humidity_gauges[i].labels(sensor_name=sensor_name).set(humidity)
                            #logging.debug(f"Set humidity for {sensor_name} to {humidity}")
                        except ValueError:
                            logging.error(f"Invalid humidity value for {humidity_key}: {humidity_value}")
                            humidity_gauges[i].labels(sensor_name=sensor_name).set(float('nan'))
                    else:
                        logging.warning(f"Missing or 'None' value for {humidity_key}")
                        humidity_gauges[i].labels(sensor_name=sensor_name).set(float('nan'))

    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == '__main__':
    # Start Prometheus metrics server
    start_http_server(8000)
    csv_file_path = '/mnt/ramdisk/weewx.csv'

    while True:
        process_csv(csv_file_path)
        time.sleep(15)

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

# Define Prometheus Gauges with 'sensor_name' as a label
temperature_gauge = Gauge('temperature', 'Temperature sensor in Celsius', ['sensor_name'])
humidity_gauge = Gauge('humidity', 'Humidity sensor in percent', ['sensor_name'])
data_age_gauge = Gauge('data_age_seconds', 'Age of the data in seconds')

garbage_data_counter = 0  # Counter to track garbage data occurrences
previous_values = {sensor_name: {'temp': float('nan'), 'humidity': float('nan')} for sensor_name in SENSOR_NAMES}

# Default temp readings are in F, needs to be converted to C
def fahrenheit_to_celsius(fahrenheit):
    try:
        return round((fahrenheit - 32) * 5.0 / 9.0, 1)  # Modified to round temperature to 1 decimal place
    except TypeError:
        logging.error(f"Invalid type for temperature conversion: {fahrenheit}")
        return float('nan')

def process_csv(file_path):
    global garbage_data_counter, previous_values  # Access global variables
    try:
        #logging.debug(f"Attempting to read the CSV file: {file_path}")
        with open(file_path, 'r', newline='') as csvfile:
            #logging.debug("CSV file opened successfully.")
            # Read the csv
            reader = csv.DictReader(csvfile)
            # Remove # from labels
            reader.fieldnames = [name.lstrip('#').strip() for name in reader.fieldnames]
            #logging.debug(f"Adjusted fieldnames: {reader.fieldnames}")

            # Check if garbage data (missing batteryStatus0 label in header)
            if 'batteryStatus0' not in reader.fieldnames:
                logging.warning("Garbage data detected in the CSV file.")
                garbage_data_counter += 1

                # If garbage data is met the second time, export NaN
                if garbage_data_counter > 1:
                    for sensor_name in SENSOR_NAMES:
                        temperature_gauge.labels(sensor_name=sensor_name).set(float('nan'))
                        humidity_gauge.labels(sensor_name=sensor_name).set(float('nan'))
                else:
                    # If garbage data is met once, export previous values
                    for sensor_name in SENSOR_NAMES:
                        temperature_gauge.labels(sensor_name=sensor_name).set(previous_values[sensor_name]['temp'])
                        humidity_gauge.labels(sensor_name=sensor_name).set(previous_values[sensor_name]['humidity'])
                return

            # Reset garbage data counter if valid data is processed
            garbage_data_counter = 0

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
                    for sensor_name in SENSOR_NAMES:
                        temperature_gauge.labels(sensor_name=sensor_name).set(float('nan'))
                        humidity_gauge.labels(sensor_name=sensor_name).set(float('nan'))
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
                            temperature_gauge.labels(sensor_name=sensor_name).set(temp_c)
                            previous_values[sensor_name]['temp'] = temp_c  # Store valid temperature value
                            #logging.debug(f"Set temperature for {sensor_name} to {temp_c}")
                        except ValueError:
                            logging.error(f"Invalid temperature value for {temp_key}: {temp_value}")
                            temperature_gauge.labels(sensor_name=sensor_name).set(float('nan'))
                    else:
                        logging.warning(f"Missing or 'None' value for {temp_key}")
                        temperature_gauge.labels(sensor_name=sensor_name).set(float('nan'))

                    # Process humidity
                    humidity_value = row.get(humidity_key)
                    if humidity_value and humidity_value.lower() != 'none':
                        try:
                            humidity = int(float(humidity_value))  # Modified to convert humidity to integer
                            humidity_gauge.labels(sensor_name=sensor_name).set(humidity)
                            previous_values[sensor_name]['humidity'] = humidity  # Store valid humidity value
                            #logging.debug(f"Set humidity for {sensor_name} to {humidity}")
                        except ValueError:
                            logging.error(f"Invalid humidity value for {humidity_key}: {humidity_value}")
                            humidity_gauge.labels(sensor_name=sensor_name).set(float('nan'))
                    else:
                        logging.warning(f"Missing or 'None' value for {humidity_key}")
                        humidity_gauge.labels(sensor_name=sensor_name).set(float('nan'))

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

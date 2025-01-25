## prom-exporter

Prometheus exporter for CSV file

## CSV file

- CSV file has header line and single data line 
- CSV file is constantly truncated every ~10 secs with updated data line
- CSV data line consists several temperature and humidity readings 
temp0 and hum0 come from same sensor
temp1 and hum1 come from another sensor and so on

## This Python exporter (weewx_to_prom.py)

- This Python script reads the temp and humidity every X seconds (last line of the script) and exports it in Prometheus compatible format at http://localhost:8000/metrics
- Then Prometheus server can be configured with a new job to come and scrape the metrics

### Sample CSV used for input to this script (weewx.csv)
### Sample export produced by this script (export.prom)
### Initial ChatGPT prompt which made fairly good version of the needed python code! (chatGPT.txt)
#### prom-exporter

Prometheus exporter for CSV file

##### CSV file

- CSV file has header line and single data line 
- CSV file is constantly truncated every ~10 secs with updated data line
- CSV data line consists several temperature and humidity readings 
temp0 and hum0 come from same sensor
temp1 and hum1 come from another sensor and so on

##### This Python exporter

- This Python script reads the temp and humidity and exports it in Prometheus compatible format at http://localhost:8000/metrics
- Then Prometheus server can be configured with a new job to come and scrape the metrics

Bitso Order Book Monitor

This Python script monitors the bid-ask spread from the order books btc_mxn and usd_mxn on the Bitso exchange. It performs custom analysis on the bid-ask spread and creates alerts whenever the spread exceeds certain thresholds. The script gathers order book data every second and stores it in files, with each file containing 600 records corresponding to a 10-minute duration.

Partitioning key
As a regular practice while batch loading files into s3 for datapipelines I use a combination of directory_path and timestamp to identify batches since its easier to identify rather than having an id directory or a secuential id structure.

Requirements
Python 3
Requests library (install via pip install requests)

Usage
Clone the repository or download the script bitso_order_book_monitor.py.
Ensure you have Python 3 installed on your system.
Install the Requests library if you haven't already (pip install requests).
Update the books list to include the order books you want to monitor.
Set the output_directory variable to specify the directory where the output files will be stored.
Set the duration variable to specify the duration (in seconds) for which the monitoring should run.
Run the script using the command python bitso_order_book_monitor.py.

Configuration
The script uses the Bitso API to fetch order book data. Ensure you have an internet connection to access the API endpoint.
Adjust the interval in the time.sleep() function to control the frequency of data collection.

File Structure
The script generates files in JSON format, with each file containing order book data for a 10-minute duration. The files are stored in the specified output_directory. Each file is named according to its creation timestamp.
# Description

This is an ETL pipeline for taking `metro` water depth data for monitoring wells and loading it into the KWB data warehouse.

Once data is downloaded from `metro` in `.csv` format, the files are dumped in the `data_dump` folder. From here, the ETL is ran following morning via Task Scheduler and the Power Shell script.

# Requirements

Currently this relies on one package, `kwb_loader`, which will install three dependies:

1. `psychopg2`
2. `polars`
3. `numpy`

Please see the requirements.txt file for specific versions.

# Operations

Data from `metro` contains Delivery data which we don't care about for the monitoring wells. To get around this, the file is read and everything past a line containing the word 'Delivery' is thrown out. From there, the date is added to the data for meta data purposes, the columns are renamed, and rows with no values in the measurement column are dropped. 

The data is then written as a `.parquet` file and loaded into the database.
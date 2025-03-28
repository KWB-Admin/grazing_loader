# Description

This is an ETL pipeline for taking `grazing` data for land managment and loading it into the KWB data warehouse.

Once data is downloaded from pdf's sent by Jim Jones or another associate, these are saved in the `data_dump` folder and then processed for loading. As these are pdfs, this is done via ocr and Tesseract.

# Requirements

This software relies on these dependies:

1. `psychopg2`
2. `polars`
3. `numpy`
4. `PyYAML`
5. `requests`
6. `img2table`

Please see the requirements.txt file for specific versions.

# Operations

Grazing data is delievered via pdf. Although this could be sent as a spreadsheet, pdf seems to be preferred from Jim Jones as it renders a report. 

After processing via Tesseract, relevant data is extracted from the resulting table and table headers, as well as cleaned in terms of grazing area name.
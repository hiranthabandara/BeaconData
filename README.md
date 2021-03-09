## BeaconData

In this exercise, our goal is to read an Excel file with multiple tabs, perform some minor transformations on the data and write the results to a set of tables in the Redshift database.

Prerequisites

Python 2.7
pandas, boto3 and python-psycopg2 modules are installed
Code downloaded and extracted to a local folder from https://github.com/hiranthabandara/BeaconData
Rename config_tmp.ini to config.ini and update S3 and Redshift credentials
Make sure the S3 bucket contains the 'Input.xlsx'

Note: Code developed and tested on Ubuntu 14.04

How to run?

Run main.py

How does it work?

Code is designed to list the *.xlsx files in the S3 bucket. Each tab in the sheet will be processed through the relevant handler. If any of the handlers fail, still code will continue to work. Any new tabs other than the supported are ignored in this implementation.

![Design](https://user-images.githubusercontent.com/79296007/110528480-33af1600-813e-11eb-9e58-3ef3b56f65ab.png)



Further Improvements

Set up a shell script for this code, then this can be run as a daemon service (scheduler). So the code will run at a certain interval and update the tables with the latest data.
Update DefaultSheetHandler to write the unsupported tabs into a different file/table so it can be processed later.
Notify admin user about the failed database/S3 connectivities through an email.


Readings

https://www.sqlshack.com/getting-started-with-amazon-s3-and-python/
https://docs.aws.amazon.com/redshift/latest/mgmt/connecting-using-workbench.html
https://www.w3schools.com/python
https://www.w3schools.com/python/pandas/default.asp

https://stackoverflow.com/questions/37354105/find-the-end-of-the-month-of-a-pandas-dataframe-series
https://stackoverflow.com/questions/19379120/how-to-read-a-config-file-using-python

https://github.com/hiranthabandara/BeaconData
https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-source-s3.html
https://support.sisense.com/hc/en-us/community/posts/360037984993-Using-Python-to-Write-a-Create-Table-Statement-and-Load-a-CSV-into-Redshift

https://stackabuse.com/the-factory-method-design-pattern-in-python

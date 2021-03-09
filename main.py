#!/usr/bin/env python2.7

import boto3
import io
import pandas
import logging
from ConfigParser import SafeConfigParser
import dao
from handlerFactory import *

# Defining the logging config
logging.basicConfig(filename='test.log',level=logging.DEBUG)

# Read the app configs
config = SafeConfigParser()
config.read('config.ini')
awsAccessKeyId = config.get('S3', 'aws_access_key_id')
awsSecretAccessKey = config.get('S3', 'aws_secret_access_key')
regionName = config.get('S3', 'region_name')

# Create permanent tables
dao.createSchema(config)

# Create temp tables (drop these each time the program is running)
dao.createTempSalesSchema(config)
dao.createTempInventorySchema(config)

# Create an instance of handler factory
handlerFactory = HandlerFactory()

# Create the S3 client and loop through the contents of the bucket to filter out *.xlsx files.
# Each sheet of the excel file will be processed by the relevant handler
client = boto3.client('s3',aws_access_key_id = awsAccessKeyId,aws_secret_access_key = awsSecretAccessKey,region_name = regionName)

resp = client.list_objects_v2(Bucket=config.get('S3', 'bucketName'))
for obj in resp['Contents']:
   key = obj['Key']
   if key.endswith('.xlsx'):
      # Create the S3 object
      obj = client.get_object(Bucket = config.get('S3', 'bucketName'), Key = key)
      # Read data from the S3 object
      print("\n**********")	
      print("Starts processing the file : " + key)
      data = obj['Body'].read()
      ioBytes = io.BytesIO(data)
      x1 = pandas.ExcelFile(ioBytes)
      for sheetName in x1.sheet_names:
  	handler = handlerFactory.getHandler(client, config, sheetName, data, fileName=key)
	# Make sure that the other handlers are working even though one fails.
	try:  	
	  handler.process()	
	except:
	  logging.error('Something went wrong with the handler %s', handler)

	

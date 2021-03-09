from abc import ABCMeta, abstractmethod
import boto3
import io
import pandas
import logging
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import MonthBegin
import re
from datetime import datetime
import csv
from botocore.exceptions import ClientError
from ConfigParser import SafeConfigParser
import dao
from handlerFactory import *
import uuid
import os
import psycopg2 as pg
import hashlib
from datetime import datetime
from six import string_types


# Handler factory class to get a specific handler
class HandlerFactory:
    def getHandler(self, client, config, name, data,  fileName): 
	print("\nGetting handler for : " + name)        
	if name == 'SKU by Channel':
            return ChannelBySKUSheetHandler(client, config, data, name, fileName)
        elif name == 'Channel by State':
            return ChannelByStateSheetHandler(client, config, data, name, fileName)
	elif name == 'Store':
	    return StoreSheetHandler(client, config, data, name, fileName)		
	elif name == 'Sub Distributor':
	    return SubDistributorSheetHandler(client, config, data, name, fileName)
        elif name == 'Inventory':
            return InventorySheetHandler(client, config, data, name, fileName)
 	else:
	    return DefaultSheetHandler(client, config, data, name, fileName)	

# Base handler class
class SheetHandler():
    __metaclass__ = ABCMeta

    # process the sheet
    @abstractmethod
    def process(self):
        pass

    # Process the common columns in the sheet
    def processCommonColumns(self, rowList, fileName, sheetName, rowNumber):	
	sha1Obj = hashlib.sha1()
        sha1Obj.update(self.fileName)
	sha1Obj.update(self.name)
	sha1Obj.update(rowNumber)
	uuidVal = sha1Obj.hexdigest()
        rowList.append(uuidVal)
			
	createdAt = datetime.utcnow().strftime("%Y-%m-%d")
	rowList.append(createdAt)

	rowList.append(self.fileName)
	rowList.append(self.name)
    
    # Process the month column. If populateStartDate = True then derive the start date of the month
    def processMonthColumn(self, rowList, row, index, columnName, populateStartDate):
	monthStr = row[columnName]
	if(isinstance(monthStr, string_types)):		
	  monthStr = monthStr.strip() # Remove leading and trailing whitespaces

        monthBegin = ''
        monthEnd = ''
	if(isEmpty(monthStr)) :
	   logging.warning('%s of row %s is empty ', columnName, index)
	else:
	   monthStr = re.sub('-M','', monthStr)
	   monthStr = re.sub(' \(.*\)','', monthStr)	    
	   monthEnd = pandas.to_datetime(monthStr, format="%Y%m") + MonthEnd(0) # get the end date of the month
	   monthEnd = re.sub(' 00:00:00','',str(monthEnd)) # remove time part from the date

	   if(populateStartDate):
	     	monthBegin = pandas.to_datetime(monthStr, format="%Y%m") + MonthBegin(0) # get the start date of the month
	     	monthBegin = re.sub(' 00:00:00','',str(monthBegin)) # remove time part from the date
	   	rowList.append(monthBegin)

	   rowList.append(monthEnd)
    
    # process the given columnName	
    def processColumn(self, rowList, row, index, columnName):
	colVal = row[columnName]
	if(isinstance(colVal, string_types)):
	  colVal = colVal.strip()
	
	if(isEmpty(colVal)):
	  logging.warning('%s of row %s is empty ', columnName, index)

	rowList.append(colVal)

# Handler for the channel by SKU sheet
class ChannelBySKUSheetHandler(SheetHandler):
    def __init__(self, client, config, data, name, fileName):
        self.data = data
        self.name = name
        self.fileName = fileName
        self.client = client
        self.config = config

    def process(self):
	print("ChannelBySKUSheetHandler starts processing..")    
	df = pandas.read_excel(io.BytesIO(self.data), sheetname=self.name, encoding='utf-8')
        df.fillna('', inplace = True)
	outputFileName = 'ChannelBySKU.csv'	
	with open(outputFileName, 'wb') as file: # will overwrite any existing content
	  writer = csv.writer(file)	
	  rowCount = 0 # The number of records in the current tab excluding the skipped	
	  for index, row in df.iterrows():
	    rowList = []
						
	    rowNumber = index
            
	    super(ChannelBySKUSheetHandler, self).processCommonColumns(rowList, self.fileName, self.name, rowNumber)
    	    super(ChannelBySKUSheetHandler, self).processMonthColumn(rowList, row, index, 'Fiscal Month', True)
	    super(ChannelBySKUSheetHandler, self).processColumn(rowList, row, index, 'Distribution Channel')
	    super(ChannelBySKUSheetHandler, self).processColumn(rowList, row, index, 'Material Code')
            super(ChannelBySKUSheetHandler, self).processColumn(rowList, row, index, 'Vendor Material Code')
	    super(ChannelBySKUSheetHandler, self).processColumn(rowList, row, index, 'Net Sales Qty')
	    super(ChannelBySKUSheetHandler, self).processColumn(rowList, row, index, 'Net Sls Sd')

	    typeVal = self.config.get('ChannelBySKU', 'type')
	    rowList.append(typeVal)	

	    writer.writerow(rowList)
	    rowCount = rowCount + 1
        file.close()	# closing the file
	print("ChannelBySKUSheetHandler created the "+file.name+"....")    
	
	# upload the file
        uploadFile(self.client, self.config.get('S3', 'bucketName'), file.name)
	print("ChannelBySKUSheetHandler uploaded the "+file.name+" ......")

	# copy to temp table
	statment = "COPY testdb.de_100.sales_temp (uuid, created_at, file_name, sheet_name, reporting_period_start, reporting_period_end, sell_through_channel, product_retailer_sku, product_sku, total_quantity, total_value, type) from 's3://"+self.config.get('S3', 'bucketName')+"/"+outputFileName+"' credentials 'aws_access_key_id="+self.config.get('S3', 'aws_access_key_id')+";aws_secret_access_key="+self.config.get('S3', 'aws_secret_access_key')+"' emptyasnull blanksasnull fillrecord csv;"
	dao.executeSQL(self.config, statment, None)
	print("ChannelBySKUSheetHandler populated the temp table testdb.de_100.sales_temp........")
	
	# update the number of records
	statment = "UPDATE testdb.de_100.sales_temp SET num_records = %s where file_name = %s and sheet_name = %s;"
	parameters = [rowCount, self.fileName, self.name]
	dao.executeSQL(self.config, statment, parameters)

	# move new records from temp to permanent table
	statment = "insert into testdb.de_100.sales (select * from testdb.de_100.sales_temp where uuid in (select uuid from testdb.de_100.sales_temp except select uuid from testdb.de_100.sales));"
	dao.executeSQL(self.config, statment, None)
	print("ChannelBySKUSheetHandler populated the permanent table testdb.de_100.sales..........")

	print("ChannelBySKUSheetHandler ends processing............")  
	return df

# Handler for the channel by state sheet
class ChannelByStateSheetHandler(SheetHandler):
    def __init__(self, client, config, data, name, fileName):
        self.data = data
        self.name = name
        self.fileName = fileName
        self.client = client
        self.config = config

    def process(self):
	print("ChannelByStateSheetHandler starts processing..")
	df = pandas.read_excel(io.BytesIO(self.data), sheetname=self.name, encoding='utf-8')
        df.fillna('', inplace = True)
	outputFileName = 'ChannelByState.csv'	
	with open(outputFileName, 'wb') as file: # will overwrite any existing content
	  writer = csv.writer(file)	
	  rowCount = 0 # The number of records in the current tab excluding the skipped	
	  for index, row in df.iterrows():
	    rowList = []
						
	    rowNumber = index
            
	    super(ChannelByStateSheetHandler, self).processCommonColumns(rowList, self.fileName, self.name, rowNumber)
    	    super(ChannelByStateSheetHandler, self).processMonthColumn(rowList, row, index, 'Fiscal Month', True)
	    super(ChannelByStateSheetHandler, self).processColumn(rowList, row, index, 'Distribution Channel')
	    super(ChannelByStateSheetHandler, self).processColumn(rowList, row, index, 'Ship to State')
	    super(ChannelByStateSheetHandler, self).processColumn(rowList, row, index, 'Net Sales Qty')
	    super(ChannelByStateSheetHandler, self).processColumn(rowList, row, index, 'Net Sls Sd')

	    typeVal = self.config.get('ChannelByState', 'type')
	    rowList.append(typeVal)	

	    writer.writerow(rowList)
	    rowCount = rowCount + 1
        file.close()	# closing the file
	print("ChannelByStateSheetHandler created the "+file.name+"....")
    	
	# upload the file
        uploadFile(self.client, self.config.get('S3', 'bucketName'), file.name)
	print("ChannelByStateSheetHandler uploaded the "+file.name+" ......")

	# copy to temp table
	statment = "COPY testdb.de_100.sales_temp (uuid, created_at, file_name, sheet_name, reporting_period_start, reporting_period_end, sell_through_channel, state, total_quantity, total_value, type) from 's3://"+self.config.get('S3', 'bucketName')+"/"+outputFileName+"' credentials 'aws_access_key_id="+self.config.get('S3', 'aws_access_key_id')+";aws_secret_access_key="+self.config.get('S3', 'aws_secret_access_key')+"' emptyasnull blanksasnull fillrecord csv;"
	dao.executeSQL(self.config, statment, None)
	print("ChannelByStateSheetHandler populated the temp table testdb.de_100.sales_temp........")
	
	# update the number of records
	statment = "UPDATE testdb.de_100.sales_temp SET num_records = %s where file_name = %s and sheet_name = %s;"
	parameters = [rowCount, self.fileName, self.name]
	dao.executeSQL(self.config, statment, parameters)

	# move new records from temp to permanent table
	statment = "insert into testdb.de_100.sales (select * from testdb.de_100.sales_temp where uuid in (select uuid from testdb.de_100.sales_temp except select uuid from testdb.de_100.sales));"
	dao.executeSQL(self.config, statment, None)
	print("ChannelByStateSheetHandler populated the permanent table testdb.de_100.sales..........")
	print("ChannelByStateSheetHandler ends processing............")  

	return df

# Handler for the store sheet
class StoreSheetHandler(SheetHandler):
    def __init__(self, client, config, data, name, fileName):
        self.data = data
        self.name = name
        self.fileName = fileName
        self.client = client
        self.config = config

    def process(self):
	print("StoreSheetHandler starts processing..")    
	df = pandas.read_excel(io.BytesIO(self.data), sheetname=self.name, encoding='utf-8')
	df.fillna('', inplace = True)
	outputFileName = 'Store.csv'	
	with open(outputFileName, 'wb') as file: # will overwrite any existing content
	  writer = csv.writer(file)	
	  rowCount = 0 # The number of records in the current tab excluding the skipped	
	  for index, row in df.iterrows():
	    rowList = []
						
	    rowNumber = index
            
	    super(StoreSheetHandler, self).processCommonColumns(rowList, self.fileName, self.name, rowNumber)
    	    super(StoreSheetHandler, self).processMonthColumn(rowList, row, index, 'Fiscal Month', True)
	    super(StoreSheetHandler, self).processColumn(rowList, row, index, 'Distribution Channel')
	    super(StoreSheetHandler, self).processColumn(rowList, row, index, 'Profit Center Code')
            super(StoreSheetHandler, self).processColumn(rowList, row, index, 'Profit Center')
	    super(StoreSheetHandler, self).processColumn(rowList, row, index, 'Net Sales Qty')
	    super(StoreSheetHandler, self).processColumn(rowList, row, index, 'Net Sls Sd')

	    typeVal = self.config.get('Store', 'type')
	    rowList.append(typeVal)	

	    writer.writerow(rowList)
	    rowCount = rowCount + 1
        file.close()	# closing the file
	print("StoreSheetHandler created the "+file.name+"....")    
	
	# upload the file
        uploadFile(self.client, self.config.get('S3', 'bucketName'), file.name)
	print("StoreSheetHandler uploaded the "+file.name+" ......")

	# copy to temp table
	statment = "COPY testdb.de_100.sales_temp (uuid, created_at, file_name, sheet_name, reporting_period_start, reporting_period_end, sell_through_channel, store_id, store_name, total_quantity, total_value, type) from 's3://"+self.config.get('S3', 'bucketName')+"/"+outputFileName+"' credentials 'aws_access_key_id="+self.config.get('S3', 'aws_access_key_id')+";aws_secret_access_key="+self.config.get('S3', 'aws_secret_access_key')+"' emptyasnull blanksasnull fillrecord csv;"
	dao.executeSQL(self.config, statment, None)
	print("StoreSheetHandler populated the temp table testdb.de_100.sales_temp........")

	# update the number of records
	statment = "UPDATE testdb.de_100.sales_temp SET num_records = %s where file_name = %s and sheet_name = %s;"
	parameters = [rowCount, self.fileName, self.name]
	dao.executeSQL(self.config, statment, parameters)

	# move new records from temp to permanent table
	statment = "insert into testdb.de_100.sales (select * from testdb.de_100.sales_temp where uuid in (select uuid from testdb.de_100.sales_temp except select uuid from testdb.de_100.sales));"
	dao.executeSQL(self.config, statment, None)
	print("StoreSheetHandler populated the permanent table testdb.de_100.sales..........")
	print("StoreSheetHandler ends processing............")  

	return df

# Handler for the Sub Distributor sheet
class SubDistributorSheetHandler(SheetHandler):
    def __init__(self, client, config, data, name, fileName):
        self.data = data
        self.name = name
        self.fileName = fileName
        self.client = client
        self.config = config

    def process(self):
	print("SubDistributorSheetHandler starts processing..")     
	df = pandas.read_excel(io.BytesIO(self.data), sheetname=self.name, encoding='utf-8')
	df.fillna('', inplace = True)
	outputFileName = 'SubDistributor.csv'	
	with open(outputFileName, 'wb') as file: # will overwrite any existing content
	  writer = csv.writer(file)	
	  rowCount = 0 # The number of records in the current tab excluding the skipped	
	  for index, row in df.iterrows():
	    rowList = []
						
	    rowNumber = index
            
	    super(SubDistributorSheetHandler, self).processCommonColumns(rowList, self.fileName, self.name, rowNumber)
    	    super(SubDistributorSheetHandler, self).processMonthColumn(rowList, row, index, 'Fiscal Month', True)
	    super(SubDistributorSheetHandler, self).processColumn(rowList, row, index, 'Distribution Channel')
	    super(SubDistributorSheetHandler, self).processColumn(rowList, row, index, 'Cust Lvl 4')
	    super(SubDistributorSheetHandler, self).processColumn(rowList, row, index, 'Net Sales Qty')
	    super(SubDistributorSheetHandler, self).processColumn(rowList, row, index, 'Net Sls Sd')

	    typeVal = self.config.get('SubDistributor', 'type')
	    rowList.append(typeVal)	

	    writer.writerow(rowList)
	    rowCount = rowCount + 1
        file.close()	# closing the file
	print("SubDistributorSheetHandler created the "+file.name+"....")
    	
	# upload the file
        uploadFile(self.client, self.config.get('S3', 'bucketName'), file.name)
	print("SubDistributorSheetHandler uploaded the "+file.name+" ......")

	# copy to temp table
	statment = "COPY testdb.de_100.sales_temp (uuid, created_at, file_name, sheet_name, reporting_period_start, reporting_period_end, sell_through_channel, store_name, total_quantity, total_value, type) from 's3://"+self.config.get('S3', 'bucketName')+"/"+outputFileName+"' credentials 'aws_access_key_id="+self.config.get('S3', 'aws_access_key_id')+";aws_secret_access_key="+self.config.get('S3', 'aws_secret_access_key')+"' emptyasnull blanksasnull fillrecord csv;"
	dao.executeSQL(self.config, statment, None)
	print("SubDistributorSheetHandler populated the temp table testdb.de_100.sales_temp........")

	# update the number of records
	statment = "UPDATE testdb.de_100.sales_temp SET num_records = %s where file_name = %s and sheet_name = %s;"
	parameters = [rowCount, self.fileName, self.name]
	dao.executeSQL(self.config, statment, parameters)

	# move new records from temp to permanent table
	statment = "insert into testdb.de_100.sales (select * from testdb.de_100.sales_temp where uuid in (select uuid from testdb.de_100.sales_temp except select uuid from testdb.de_100.sales));"
	dao.executeSQL(self.config, statment, None)
	print("SubDistributorSheetHandler populated the permanent table testdb.de_100.sales..........")
	print("SubDistributorSheetHandler ends processing............")  

	return df

# Handler for the Inventory sheet
class InventorySheetHandler(SheetHandler):
    def __init__(self, client, config, data, name, fileName):
        self.data = data
        self.name = name
        self.fileName = fileName
        self.client = client
        self.config = config

    def process(self):
	print("InventorySheetHandler starts processing..")   
	df = pandas.read_excel(io.BytesIO(self.data), sheetname=self.name, encoding='utf-8')
	df.fillna('', inplace = True)
	outputFileName = 'Inventory.csv'	
	with open(outputFileName, 'wb') as file: # will overwrite any existing content
	  writer = csv.writer(file)	
	  rowCount = 0 # The number of records in the current tab excluding the skipped	
	  for index, row in df.iterrows():
	    rowList = []
						
	    rowNumber = index
            
	    super(InventorySheetHandler, self).processCommonColumns(rowList, self.fileName, self.name, rowNumber)
  	    super(InventorySheetHandler, self).processMonthColumn(rowList, row, index, 'MONTH', False)
	    super(InventorySheetHandler, self).processColumn(rowList, row, index, 'Plant')
	    super(InventorySheetHandler, self).processColumn(rowList, row, index, 'Material Code')
	    super(InventorySheetHandler, self).processColumn(rowList, row, index, 'Vendor Material Code')
	    super(InventorySheetHandler, self).processColumn(rowList, row, index, 'MATERIAL DESC')
	    super(InventorySheetHandler, self).processColumn(rowList, row, index, 'Inv Total Qty')
	
	    typeVal = self.config.get('Inventory', 'type')
	    rowList.append(typeVal)	

	    writer.writerow(rowList)
	    rowCount = rowCount + 1
        file.close()	# closing the file
	print("InventorySheetHandler created the "+file.name+"....")
    	
	# upload the file
        uploadFile(self.client, self.config.get('S3', 'bucketName'), file.name)
	print("InventorySheetHandler uploaded the "+file.name+" ......")

	# copy to temp table
	statment = "COPY testdb.de_100.inventory_temp (uuid, created_at, file_name, sheet_name, effective_date, plant_name, product_retailer_sku, product_sku, product_name, quantity_warehouse, type) from 's3://"+self.config.get('S3', 'bucketName')+"/"+outputFileName+"' credentials 'aws_access_key_id="+self.config.get('S3', 'aws_access_key_id')+";aws_secret_access_key="+self.config.get('S3', 'aws_secret_access_key')+"' emptyasnull blanksasnull fillrecord csv;"
	dao.executeSQL(self.config, statment, None)
	print("InventorySheetHandler populated the temp table testdb.de_100.inventory_temp........")

	# update the number of records
	statment = "UPDATE testdb.de_100.inventory_temp SET num_records = %s where file_name = %s and sheet_name = %s;"
	parameters = [rowCount, self.fileName, self.name]
	dao.executeSQL(self.config, statment, parameters)

	# move new records from temp to permanent table
	statment = "insert into testdb.de_100.inventory (select * from testdb.de_100.inventory_temp where uuid in (select uuid from testdb.de_100.inventory_temp except select uuid from testdb.de_100.inventory));"
	dao.executeSQL(self.config, statment, None)
	print("InventorySheetHandler populated the permanent table testdb.de_100.inventory..........")
	print("InventorySheetHandler ends processing............")  

	return df

# Default handler for undefined sheets
class DefaultSheetHandler(SheetHandler):
    def __init__(self, client, config, data, name, fileName):
        self.data = data
        self.name = name
        self.fileName = fileName
	self.client = client
        self.config = config

    def process(self):
	#TODO: Write this to a separate file, so this can be picked by a different pipeline	
	print("DefaultSheetHandler starts processing for sheet : "+name+"..")	
	return pandas.DataFrame()
	print("DefaultSheetHandler ends processing............")  


# Checks give string value is a not None and not empty
def isEmpty(val):
  if val is None or (isinstance(val, string_types) and len(val) == 0):
    return True
  else:
    return False

# Uploads the given file to S3
def uploadFile(client, basket, fileName):
  try:
    response = client.upload_file(fileName, basket, fileName)
  except ClientError as e:
    logging.error(e)

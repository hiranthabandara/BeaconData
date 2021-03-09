CREATE TABLE IF NOT EXISTS testdb.de_100.sales(
  	uuid varchar(255) PRIMARY KEY,
	created_at date,
	file_name varchar(255),
	sheet_name varchar(255),
	num_records int,
	reporting_period_start varchar(255),
	reporting_period_end varchar(255),
	sell_through_channel varchar(255),
	store_id varchar(255),
	store_name varchar(255),	
	region varchar(255),
	country varchar(255),
	state varchar(255),
	product_retailer_sku varchar(255),
	product_sku varchar(255),
	product_name varchar(255),
	product_size varchar(255),
	product_line varchar(255),
	currency varchar(255),
	total_quantity Decimal(20,2),
	total_value Decimal(20,2),
	type varchar(255));

CREATE TABLE IF NOT EXISTS testdb.de_100.inventory(
	uuid varchar(255) PRIMARY KEY,
	created_at date,
	file_name varchar(255),
	sheet_name varchar(255),
	num_records int,
	effective_date varchar(255),
	plant_Id varchar(255),
	plant_name varchar(255),
	region varchar(255),
	country varchar(255),
	state varchar(255),
	product_retailer_sku varchar(255),
	product_sku varchar(255),
	product_name varchar(255),
	product_size varchar(255),
	product_line varchar(255),
	currency varchar(255),
	quantity_warehouse Decimal(20,2),
	type varchar(255));



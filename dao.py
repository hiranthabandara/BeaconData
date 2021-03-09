import os
import psycopg2 as pg

# Get a redshift connection
def getConnection(config):
    userName=config.get('RedShift', 'username')
    portVal=config.get('RedShift', 'port')
    databaseName=config.get('RedShift', 'database_name')
    hostName=config.get('RedShift', 'host')
    psw=config.get('RedShift', 'password')

    try:
    	return pg.connect(dbname=databaseName, host=hostName, port=portVal, user=userName, password=psw)
    except Exception as e:
	raise e

# create the schema
def createSchema(config, schema="schema.sql"):
    conn = getConnection(config)
    with open(schema, 'r') as f:
        sql = f.read()	
        try:
	    curs = conn.cursor()     
	    curs.execute(sql)
	    conn.commit()
        except Exception as e:
	    conn.rollback()
	    raise e
    conn.close()

# create the temp inventory schema
def createTempInventorySchema(config, schema="temp_inventory_schema.sql"):
    createSchema(config,schema)

# create the temp sales schema
def createTempSalesSchema(config, schema="temp_sales_schema.sql"):
    createSchema(config,schema)

# execute the given SQL
def executeSQL(config, sql, parameters):
    conn = getConnection(config)
    try:
       curs = conn.cursor()     
       if parameters is None:
          curs.execute(sql)
       else:
          curs.execute(sql, parameters)
       conn.commit()
    except Exception as e:
       conn.rollback()
       raise e
    conn.close()


   

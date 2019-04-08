import json
import sys
import mysql.connector
import pandas
import sqlalchemy
from sqlalchemy import create_engine

try:
    with open('config.json') as config_file:
        config = json.load(config_file)

except FileNotFoundError:
    print("Please create the configuration file (config.json). Reference config.initial.json for parameters.")
    sys.exit(1)

print(config)

db_connection = mysql.connector.connect(user=config['DatabaseUser'],
                                        database=config['DatabaseName'],
                                        password=config['DatabasePassword'])

cursor = db_connection.cursor()

# load pumf table creation
with open('scripts/PUMFTableCreation.sql') as pumf_table_creation_file:
    pumf_table_creation_sql = pumf_table_creation_file.read()
    cursor.execute(pumf_table_creation_sql, multi=True)

persons_data = pandas.read_csv(config["PersonsSeedFile"])

persons_data.to_sql('pumf_person',db_connection,if_exists='append')




db_connection.close()

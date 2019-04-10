import json
import sys
import mysql.connector
import pandas
import sqlalchemy
from sqlalchemy import create_engine
import warnings
import subprocess

try:
    with open('config.json') as config_file:
        config = json.load(config_file)

except FileNotFoundError:
    print("Please create the configuration file (config.json). Reference config.initial.json for parameters.")
    sys.exit(1)


def execute_multi_sql(connection, sql):
    commands = sql.split(';')
    for command in commands:
        command_stripped = command.strip()
        if len(command_stripped) > 0:
            connection.execute(command)


engine = create_engine(
    f'mysql+pymysql://{config["DatabaseUser"]}:{config["DatabasePassword"]}@localhost/{config["DatabaseName"]}'
)

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    with engine.connect() as db_connection:
        with open('scripts/PUMFTableCreation.sql') as pumf_table_creation_file:
            pumf_table_creation_sql = pumf_table_creation_file.read()
            execute_multi_sql(db_connection, pumf_table_creation_sql)

        households_data = pandas.read_csv(config["HouseholdsSeedFile"])
        households_data.to_sql('pumf_hh', db_connection, if_exists='append', index=False)

        persons_data = pandas.read_csv(config["PersonsSeedFile"])
        persons_data.to_sql('pumf_person', db_connection, if_exists='append', index=False)

        with open('scripts/PUMFTableProcessing.sql') as pumf_table_processing_file:
            pumf_table_processing_sql = pumf_table_processing_file.read()
            execute_multi_sql(db_connection, pumf_table_processing_sql)

        with open('scripts/ControlsTableCreation.sql') as controls_table_creation_file:
            controls_table_creation_sql = controls_table_creation_file.read()
            execute_multi_sql(db_connection, controls_table_creation_sql)

        maz_data = pandas.read_csv(config["MazLevelControls"])
        maz_data.to_sql('control_totals_maz', db_connection, if_exists='append', index=False)

        taz_data = pandas.read_csv(config["TazLevelControls"])
        taz_data.to_sql('control_totals_taz', db_connection, if_exists='append', index=False)

        meta_data = pandas.read_csv(config["MetaLevelControls"])
        meta_data.to_sql('control_totals_meta', db_connection, if_exists='append', index=False)

        with open('scripts/ControlsTableProcessing.sql') as controls_table_processing_file:
            controls_table_processing_sql = controls_table_processing_file.read()
            execute_multi_sql(db_connection, controls_table_processing_sql)

print("Finished initial database setup.")

''' SET CLASSPATH=runtime\config
SET CLASSPATH=%CLASSPATH%;runtime\*
SET CLASSPATH=%CLASSPATH%;runtime\lib\*
SET CLASSPATH=%CLASSPATH%;runtime\lib\JPFF-3.2.2\JPPF-3.2.2-admin-ui\lib\*
SET LIBPATH=runtime\lib '''

classpath_root = 'runtime/config'
classpaths = [f'{classpath_root}', 'runtime/*', 'runtime/lib/*', 'runtime/lib/JPFF-3.2.2/JPPF-3.2.2-admin-ui/lib/*']

libpath = 'runtime/lib'

print(classpaths)

# %JAVA_64_PATH%\bin\java -showversion -server -Xms8000m -Xmx15000m -cp "%CLASSPATH%"
# -Djppf.config=jppf-clientLocal.properties -Djava.library.path=%LIBPATH% popGenerator.PopGenerator runtime/config/settings.xml
subprocess.run([f'{config["Java64Path"]}/bin/java', "-showversion", '-server', '-Xms8000m', '-Xmx15000m',
                '-cp', ';'.join(classpaths), '-Djppf.config=jppf-clientLocal.properties',
                f'-Djava.library.path={libpath}',
                'popGenerator.PopGenerator', 'runtime/config/settings.xml'], shell=True)

# reconnect and run GTAModelTransform
with engine.connect() as db_connection:
    with open('scripts/GTAModelTransform.sql') as gta_model_transform_file:
        gta_model_transform_sql = gta_model_transform_file.read()
        execute_multi_sql(db_connection, gta_model_transform_sql)

print("Synthesized populaton extracted and transformed into TTS-like data")

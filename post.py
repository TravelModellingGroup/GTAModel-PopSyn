import json
import sys
import mysql.connector
import pandas
import sqlalchemy
from sqlalchemy import create_engine
import warnings
import subprocess
import logzero
from logzero import logger
import logzero

# Set a logfile (all future log messages are also saved there)
logzero.logfile("output/post-process.log")

try:
    with open('config.json') as config_file:
        config = json.load(config_file)

except FileNotFoundError:
    logger.error("Please create the configuration file (config.json). Reference config.initial.json for parameters.")
    sys.exit(1)


def execute_multi_sql(connection, sql):
    '''
    Executes multiple sql statements from a single sql block
    semicolon is used as a delimiter for very simple parsing
    :param connection:
    :param sql:
    :return:
    '''
    commands = sql.split(';')
    for command in commands:
        command_stripped = command.strip()
        if len(command_stripped) > 0:
            connection.execute(command)


engine = create_engine(
    f'mysql+pymysql://{config["DatabaseUser"]}:'
    f'{config["DatabasePassword"]}@{config["DatabaseServer"]}/{config["DatabaseName"]}'
)

logger.info("GTAModel popsyn post-processing started.")

with engine.connect() as db_connection:
    with open('scripts/GTAModelTransform.sql') as gta_model_transform_file:
        gta_model_transform_sql = gta_model_transform_file.read()
        execute_multi_sql(db_connection, gta_model_transform_sql)

    logger.info("Synthesized populion data transformed.")

    gta_households = pandas.read_sql_table('gta_households', db_connection)
    gta_households.to_csv('output/gtamodel_households.csv', index=False)

    logger.info('Synthesized persons written to file: output/gtamodel_households.csv')

    gta_persons = pandas.read_sql_table('gta_persons', db_connection)
    gta_persons.to_csv('output/gtamodel_persons.csv', index=False)

    logger.info('Synthesized persons written to file: output/gtamodel_persons.csv')

    gta_zonal_residence = gta_households[['HouseholdZone', 'ExpansionFactor']] \
        .groupby('HouseholdZone').agg({'ExpansionFactor': sum}).reset_index().rename(
        columns={'HouseholdZone': 'Zone', 'ExpansionFactor': 'ExpandedHouseholds'}).to_csv(
        'output/gtamodel_zonal_residence.csv')

    logger.info("zonal residence written to file: output/gtamodel_zonal_residence.csv")

    gta_households = gta_households[['HouseholdId', 'HouseholdZone']]
    gta_persons = gta_persons[['HouseholdId', 'Occupation',
                               'ExpansionFactor', 'EmploymentZone', 'EmploymentStatus']]

    gta_ph = pandas.merge(left=gta_persons, right=gta_households, left_on='HouseholdId', right_on='HouseholdId')
    gta_ph = gta_ph.loc[gta_ph.EmploymentZone == 0]
    gta_ph = gta_ph[['HouseholdZone', 'EmploymentStatus', 'Occupation', 'ExpansionFactor']]
    gta_ph_grouped = gta_ph.groupby(['HouseholdZone', 'EmploymentStatus', 'Occupation']).agg(
        {'ExpansionFactor': sum}).reset_index()
    gta_ph_grouped.rename(columns={'HouseholdZone': 'Zone', 'ExpansionFactor': 'Persons'}, inplace=True)

    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'G') &
                       (gta_ph_grouped.EmploymentStatus == 'F')][['Zone', 'Persons']] \
        .to_csv("output/GF.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'G') &
                       (gta_ph_grouped.EmploymentStatus == 'P')][['Zone', 'Persons']] \
        .to_csv("output/GP.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'M') &
                       (gta_ph_grouped.EmploymentStatus == 'P')][['Zone', 'Persons']] \
        .to_csv("output/MP.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'M') &
                       (gta_ph_grouped.EmploymentStatus == 'F')][['Zone', 'Persons']] \
        .to_csv("output/MF.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'P') &
                       (gta_ph_grouped.EmploymentStatus == 'P')][['Zone', 'Persons']] \
        .to_csv("output/PP.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'P') &
                       (gta_ph_grouped.EmploymentStatus == 'F')][['Zone', 'Persons']] \
        .to_csv("output/PF.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'S') &
                       (gta_ph_grouped.EmploymentStatus == 'F')][['Zone', 'Persons']] \
        .to_csv("output/SF.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'S') &
                       (gta_ph_grouped.EmploymentStatus == 'P')][['Zone', 'Persons']] \
        .to_csv("output/SP.csv", index=False)

    logger.info('Employment and occuption vectors output')

    # gta_ph.to_csv('gtamodel_persons_households.csv',index=False)

logger.info("GTAModel popsyn post-processing completed.")
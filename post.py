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
import os

try:
    os.makedirs('output/HouseholdData/')
    os.makedirs('output/ZonalResidence/')
    os.makedirs('output/ZonalResidence/')
except OSError:
    pass

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

    logger.info("Synthesized population data transformed.")

    gta_households = pandas.read_sql_table('gta_households', db_connection)
    gta_households.to_csv('output/HouseholdData/Households.csv', index=False)

    logger.info('Synthesized persons written to file: output/HouseholdData/Households.csv')

    gta_persons = pandas.read_sql_table('gta_persons', db_connection)

    for mapping in config['CategoryMapping']['Persons'].items():
        inverted_map = {value: key for key, value in mapping[1].items()}

        # convert the mapping back to an integer
        gta_persons[mapping[0]] = pandas.to_numeric(gta_persons[mapping[0]])
        gta_persons[mapping[0]] = gta_persons[mapping[0]].map(inverted_map)

    gta_persons.to_csv('output/HouseholdData/Persons.csv', index=False)


    logger.info('Synthesized persons written to file: output/gtamodel_persons.csv')

    gta_zonal_residence = gta_households[['HouseholdZone', 'ExpansionFactor']] \
        .groupby('HouseholdZone').agg({'ExpansionFactor': sum}).reset_index().rename(
        columns={'HouseholdZone': 'Zone', 'ExpansionFactor': 'ExpandedHouseholds'}).to_csv(
        'output/HouseholdData/HouseholdTotals.csv',index=False)

    logger.info("zonal residence written to file: output/HouseholdData/HouseholdTotals.csv")

    del gta_zonal_residence

    gta_households = gta_households[['HouseholdId', 'HouseholdZone']]
    gta_persons = gta_persons[['HouseholdId', 'Occupation',
                               'ExpansionFactor', 'EmploymentZone', 'EmploymentStatus']]


    gta_ph = pandas.merge(left=gta_persons.copy(), right=gta_households, left_on='HouseholdId', right_on='HouseholdId')
    gta_ph = gta_ph.loc[gta_ph.EmploymentZone < 6000]
    gta_ph = gta_ph[['HouseholdZone', 'EmploymentStatus', 'Occupation', 'ExpansionFactor']]
    gta_ph = gta_ph.rename(columns={'HouseholdZone': 'Zone', 'ExpansionFactor': 'Persons'})

    zones = pandas.read_csv('data/Zones.csv')[['Zone#', 'PD']]
    gta_ph = pandas.merge(gta_ph, zones, left_on="Zone", right_on="Zone#")



    #gta_ph_grouped = gta_ph.groupby(['HouseholdZone', 'EmploymentStatus', 'Occupation']).agg(
    #    {'ExpansionFactor': sum}).reset_index()

    gta_ph_grouped  = gta_ph.groupby(['Zone', 'Occupation', 'EmploymentStatus'])['Persons'].apply(sum)
    gta_ph_grouped[:,'G','F'].reset_index().to_csv("output/ZonalResidence/GF.csv", index=False)
    gta_ph_grouped[:, 'G', 'P'].reset_index().to_csv("output/ZonalResidence/GP.csv", index=False)
    gta_ph_grouped[:, 'M', 'F'].reset_index().to_csv("output/ZonalResidence/MF.csv", index=False)
    gta_ph_grouped[:, 'M', 'P'].reset_index().to_csv("output/ZonalResidence/MP.csv", index=False)
    gta_ph_grouped[:, 'P', 'F'].reset_index().to_csv("output/ZonalResidence/PF.csv", index=False)
    gta_ph_grouped[:, 'P', 'P'].reset_index().to_csv("output/ZonalResidence/PP.csv", index=False)
    gta_ph_grouped[:, 'S', 'F'].reset_index().to_csv("output/ZonalResidence/SF.csv", index=False)
    gta_ph_grouped[:, 'S', 'P'].reset_index().to_csv("output/ZonalResidence/SP.csv", index=False)

    # pd_grouped = gta_ph.groupby(['PD', 'Occupation', 'EmploymentStatus'])['Persons'].apply(sum)
    # pd_grouped[:,'G','F'].reset_index().to_csv("output/ZonalResidencePD/ZonalResidence/GF.csv", index=False)
    # pd_grouped[:, 'G', 'P'].reset_index().to_csv("output/ZonalResidencePD/ZonalResidence/GP.csv", index=False)
    # pd_grouped[:, 'M', 'F'].reset_index().to_csv("output/ZonalResidencePD/ZonalResidence/MF.csv", index=False)
    # pd_grouped[:, 'M', 'P'].reset_index().to_csv("output/ZonalResidencePD/ZonalResidence/MP.csv", index=False)
    # pd_grouped[:, 'P', 'F'].reset_index().to_csv("output/ZonalResidencePD/ZonalResidence/PF.csv", index=False)
    # pd_grouped[:, 'P', 'P'].reset_index().to_csv("output/ZonalResidencePD/ZonalResidence/PP.csv", index=False)
    # pd_grouped[:, 'S', 'F'].reset_index().to_csv("output/ZonalResidencePD/ZonalResidence/SF.csv", index=False)
    # pd_grouped[:, 'S', 'P'].reset_index().to_csv("output/ZonalResidencePD/ZonalResidence/SP.csv", index=False)

    # set the employment zone to 0 for those not at 8888 or outside

    # gta_persons[(gta_persons['EmploymentZone'] < 6000) & (gta_persons['EmploymentZone'] != 8888)] = 0
    # gta_persons.to_csv('output/HouseholdData/Persons.csv', index=False)

    """
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'G') &
                       (gta_ph_grouped.EmploymentStatus == 'F')][['Zone', 'Persons']] \
        .to_csv("output/ZonalResidence/GF.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'G') &
                       (gta_ph_grouped.EmploymentStatus == 'P')][['Zone', 'Persons']] \
        .to_csv("output/ZonalResidence/GP.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'M') &
                       (gta_ph_grouped.EmploymentStatus == 'P')][['Zone', 'Persons']] \
        .to_csv("output/ZonalResidence/MP.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'M') &
                       (gta_ph_grouped.EmploymentStatus == 'F')][['Zone', 'Persons']] \
        .to_csv("output/ZonalResidence/MF.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'P') &
                       (gta_ph_grouped.EmploymentStatus == 'P')][['Zone', 'Persons']] \
        .to_csv("output/ZonalResidence/PP.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'P') &
                       (gta_ph_grouped.EmploymentStatus == 'F')][['Zone', 'Persons']] \
        .to_csv("output/ZonalResidence/PF.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'S') &
                       (gta_ph_grouped.EmploymentStatus == 'F')][['Zone', 'Persons']] \
        .to_csv("output/ZonalResidence/SF.csv", index=False)
    gta_ph_grouped.loc[(gta_ph_grouped.Occupation == 'S') &
                       (gta_ph_grouped.EmploymentStatus == 'P')][['Zone', 'Persons']] \
        .to_csv("output/ZonalResidence/SP.csv", index=False)
    """
    logger.info('Employment and occupation vectors output')


logger.info("GTAModel popsyn post-processing completed.")
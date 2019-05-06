from logzero import logger
import logzero
import sys
import argparse
import json
import datetime
import os
# parse input arguments

from logzero import logger, setup_logger

from gtamodel_popsyn.gtamodel_popsyn import GTAModelPopSyn

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', action='store', required=False, default='config.json',
                    help="Path of the configuration file to use.", type=argparse.FileType('r'))
parser.add_argument('-i', '--input-process-only',
                    required=False,
                    action="store_true",
                    help="Only generate synthesis files and don't run synthesis procedure.")
parser.add_argument('-d', '--database-only',
                    required=False,
                    action="store_true",
                    help="Only initialize the database and tables required for PopSyn3.")
parser.add_argument('-o', '--output-only',
                    required=False,
                    action="store_true",
                    help="Only write synthesized population from existing database data.")
parser.add_argument('-r', '--validation-report-only',
                    required=False,
                    action="store_true",
                    help="Only generate a summary report from existing output files.")
args = parser.parse_args()

try:
    config = json.load(args.config)
except:
    logger.error('Unable to load configuration file.')
    logger.info("GTAModel PopSyn will now terminate.")
    sys.exit(1)


start_time = datetime.datetime.now()
os.makedirs(f'{config["OutputFolder"]}/{start_time:%Y-%m-%d_%H-%M}/',exist_ok=True)
_logger = setup_logger('gtamodel',logfile=f'{config["OutputFolder"]}/{start_time:%Y-%m-%d_%H-%M}/gtamodel_popsyn.log')
_logger.info(f'GTAModel PopSyn')
_logger.info(f'Configuration file loaded: {args.config}')

gtamodel_popsyn = GTAModelPopSyn(config, args, start_time = start_time)

if args.database_only:
    gtamodel_popsyn.initialize_database()
    sys.exit(0)

if args.input_process_only:
    gtamodel_popsyn.generate_inputs()
    sys.exit(0)

if args.output_only:
    gtamodel_popsyn.generate_outputs()
    sys.exit(0)

if args.validation_report_only:
    gtamodel_popsyn.generate_summary_report()
    sys.exit(0)

# generating full report

gtamodel_popsyn.run()

from logzero import logger
import logzero
import sys
import argparse
import json

# parse input arguments
from gtamodel_popsyn.input_processor import InputProcessor

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', required=False, default='config.json')
args = parser.parse_args()

# initialize logger
logzero.logfile("output/pre-process.log")

logger.info(f'GTAModel PopSyn')

try:
    config = json.load(open(args.config))
except:
    logger.error('Unable to load configuration file.')
    logger.info("GTAModel PopSyn will now terminate.")
    sys.exit(1)

logger.info(f'Configuration file loaded: {args.config}')
logger.info(f'Processing input data...')

gtamodel_popsyn_input_processor = InputProcessor(config)
gtamodel_popsyn_input_processor.generate()

logger.info(f'Finished processing input data, controls and population seed records have been generated.')


logger.info('GTAModel PopSyn has completed successfully.')




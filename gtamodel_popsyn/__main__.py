from logzero import logger
import logzero
import sys
import argparse
import json

# parse input arguments
from gtamodel_popsyn.input_processor import InputProcessor
from gtamodel_popsyn.gtamodel_popsyn import GTAModelPopSyn

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', action='store', required=False, default='config.json',
                    help="Path of the configuration file to use.", type=argparse.FileType('r'))
parser.add_argument('-i', '--input-process-only',
                    required=False,
                    action="store_true",
                    help="Only generate synthesis files and don't run synthesis procedure.")
parser.add_argument('-o', '--output-only',
                    required=False,
                    action="store_true",
                    help="Only write synthesized population from existing database data.")
parser.add_argument('-r', '--report-only',
                    required=False,
                    action="store_true",
                    help="Only generate a summary report from existing output files.")
args = parser.parse_args()

# initialize logger
logzero.logfile("output/pre-process.log")

logger.info(f'GTAModel PopSyn')
logger.info(f'Configuration file loaded: {args.config}')
try:
    config = json.load(args.config)
except:
    logger.error('Unable to load configuration file.')
    logger.info("GTAModel PopSyn will now terminate.")
    sys.exit(1)

gtamodel_popsyn = GTAModelPopSyn(config)

if(args.input_process_only):
    gtamodel_popsyn.generate_inputs()
    sys.exit(0)

if args.report_only:
    gtamodel_popsyn.run_summary_report()
    sys.exit(0)

gtamodel_popsyn.run()




# gtamodel_popsyn_input_processor = InputProcessor(config)
# gtamodel_popsyn_input_processor.generate()

#logger.info(f'Finished processing input data, controls and population seed records have been generated.')

#if args.preprocess_only:
#    sys.exit(1)

#logger.info('GTAModel PopSyn has completed successfully.')

import sys
import argparse
import json
import datetime
import os
# parse input arguments
from collections import defaultdict

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
                    action="store",
                    help="Only write synthesized population from existing database data.")
parser.add_argument('-s', '--saved-output',
                    required=False,
                    action="store",
                    type=bool,
                    default=False,
                    help="Calculate employment and other output files using data already extract from the database.")
parser.add_argument('-r', '--validation-report-only',
                    required=False,
                    action="store",
                    help="Only generate a summary report from existing output files. Pass the generated output folder to use.")
parser.add_argument('-p', '--percent-population',
                    required=False,
                    action="store",
                    nargs='+',
                    default=1.0,
                    type=float,
                    help="Specify percent population")
parser.add_argument('-n', '--name',
                    required=False,
                    action="store",
                    type=str,
                    help="Assign a custom name to a run which will be prepended to the output folder location.")
parser.add_argument('-v', '--population-vector-file',
                    required=False,
                    action="store",
                    type=argparse.FileType('r'),
                    help="Path to a population vector file, will replace the population for those zones.")
parser.add_argument('-u', '--use-generated',
                    required=False,
                    action="store_true",
                    help="Use an existing set of input files.")
parser.add_argument('-t', '--use-database-controls',
                    required=False,
                    action="store_true",
                    help="Use existing control totals in database.")
parser.add_argument('-T','--use-file-controls',
                    required=False,
                    action="store",
                    metavar=('maz_controls','taz_controls','meta_controls'),
                    nargs=3)
parser.add_argument('-m', '--merge-output',
                    required=False,
                    action="store",
                    nargs=2,
                    metavar=('households_file', 'persons_file'),
                    help="Merge and (merge) multiple household and persons file when generating the output. Must be used with -u")

args = parser.parse_args()

try:
    config = json.load(args.config)
except:
    logger.error('Unable to load configuration file.')
    logger.info("GTAModel PopSyn will now terminate.")
    sys.exit(1)

start_time = datetime.datetime.now()

if args.database_only:
    gtamodel_popsyn = GTAModelPopSyn(config, args, start_time=start_time)
    gtamodel_popsyn.initialize_database()

elif args.use_database_controls:
    gtamodel_popsyn = GTAModelPopSyn(config, args, start_time=start_time, output_path=args.output_only)
    gtamodel_popsyn.post_input_run()

elif args.use_file_controls:
    gtamodel_popsyn = GTAModelPopSyn(config, args, start_time=start_time, output_path=args.output_only)
    gtamodel_popsyn.initialize_database_with_controls(args.use_file_controls[0],args.use_file_controls[1],args.use_file_controls[2])
    gtamodel_popsyn.post_input_run()

elif args.input_process_only:
    gtamodel_popsyn = GTAModelPopSyn(config, args, start_time=start_time)
    gtamodel_popsyn.generate_inputs()

elif args.use_generated:

    logger.info('Using generated input from: ' + args.output_only)
    gtamodel_popsyn = GTAModelPopSyn(config, args, start_time=start_time, output_path=args.output_only,
                                     make_output=False)

    gtamodel_popsyn.initialize_database()
    # gtamodel_popsyn.generate_outputs(use_saved=True,
    #                                 merge_outputs=args.merge_output)

elif args.output_only:
    gtamodel_popsyn = GTAModelPopSyn(config, args, start_time=start_time, output_path=args.output_only,
                                     make_output=False)
    gtamodel_popsyn.generate_outputs(args.saved_output)

elif args.validation_report_only:
    gtamodel_popsyn = GTAModelPopSyn(config, args, start_time=start_time, output_path=args.validation_report_only,
                                     make_output=False)
    gtamodel_popsyn.generate_summary_report()



else:
    gtamodel_popsyn = GTAModelPopSyn(config, args, start_time=start_time,
                                     percent_populations=[args.percent_population], name=args.name,
                                     population_vector_file=args.population_vector_file)
    gtamodel_popsyn.run()

# generating full report

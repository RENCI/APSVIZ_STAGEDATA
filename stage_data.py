#!/usr/bin/env python

import os, sys, wget
import logging
import netCDF4 as nc
from urllib.error import HTTPError
from common.logging import LoggingUtil
from urllib import parse

filelist={'zeta_max':    'maxele.63.nc', 
          'swan_HS_max': 'swan_HS_max.63.nc',
          'wind_max':    'maxwvel.63.nc', 
          'water_levels': 'fort.63.nc'}
mode = 0o755

def getDataFile(outdir, url, infilename, logger):
    '''
    '''
    # init return value
    ret_val = None

    # Get infilename and download netcdf file
    logger.debug(f"About to wget - filename: {infilename}")
    try:
        url = url if url.endswith("/") else f"{url}/"

        ret_val = wget.download(parse.urljoin(url, infilename), os.path.join(outdir, infilename))
    except HTTPError as e:
        logger.error(e)

    return ret_val

def main(args):
    '''    
    '''
    # logging.basicConfig(filename='log',format='%(asctime)s : %(levelname)s : %(funcName)s : %(module)s : %(name)s : %(message)s', level=logging.WARNING)
    # get the log level and directory from the environment
    log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
    log_path: str = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))

    # create the dir if it does not exist
    if not os.path.exists(log_path):
        os.mkdir(log_path)

    # create a logger
    logger = LoggingUtil.init_logging("APSVIZ.stage_data", level=log_level, line_format='medium',
                                      log_file_path=log_path)
    # process args
    if not args.inputURL:
        logger.info("Need inputURL on command line: --inputURL <url>.")
        return 1
    inputURL = args.inputURL.strip()

    if not args.outputDir:
        logger.info("Need output directory on command line: --output <outputdir>.")
        return 1

    if not os.path.exists(args.outputDir):
        os.makedirs(args.outputDir)

    logger.info('Input URL is {}'.format(inputURL))
    logger.info('OutputDir is {}'.format(args.outputDir))

    num = len(filelist)
    error = False

    for v in filelist:
        # grab the file
        outname = getDataFile(args.outputDir, inputURL, filelist[v], logger)

        # if there was a file gathered
        if outname is not None:
            # load the NetCDF file
            f = nc.Dataset(outname)

            # each NetCDF dimension must have a non-zero value
            for dim in f.dimensions.values():
                # is there a value
                if dim.size == 0:
                    # log the error
                    logger.error(f'Error: NetCDF Dimension "{dim.name}" for file {v} ({filelist[v]}) is invalid.')

                    # declare failure
                    error = True
        # else the file was not found
        else:
            # swan files are optional
            if not v.startswith("swan"):
                # log the error
                logger.error(f'Error: NetCDF file {v} ({filelist[v]}) was not found.')

                # declare failure
                error = True

        # if there was an error, abort
        if error:
            # no need to continue
            sys.exit(1)

    logger.info('Finished moving {} files '.format(num))

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument('--inputURL', default=None, help='URL to retrieve data from', type=str)
    parser.add_argument('--outputDir', default=None, help='Destination directory', type=str)
    
    args = parser.parse_args()

    sys.exit(main(args))

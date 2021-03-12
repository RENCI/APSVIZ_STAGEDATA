#!/usr/bin/env python

import os, sys, wget
import logging

filelist={'zeta_max':    'maxele.63.nc', 
          'swan_HS_max': 'swan_HS_max.63.nc',
          'wind_max':    'maxwvel.63.nc', 
          'water_levels': 'fort.63.nc'}
mode = 0o755

def getDataFile(outdir, url, infilename):
    '''
    '''
    # Get infilename and download netcdf file
    try:
        outfilename = wget.download(os.path.join(url,infilename), os.path.join(outdir,infilename))
        return outfilename
    except: HTTPError as e:
        logging.error(e)
        return None

def main(args):
    '''    
    '''
    logging.basicConfig(filename='log',format='%(asctime)s : %(levelname)s : %(funcName)s : %(module)s : %(name)s : %(message)s', level=logging.WARNING)
    # process args
    if not args.inputURL:
        logging.info("Need inputURL on command line: --inputURL <url>.")
        return 1
    inputURL = args.inputURL.strip()

    if not args.outputDir:
        logging.info("Need output directory on command line: --output <outputdir>.")
        return 1

    if not os.path.exists(args.outputDir):
        os.makedirs(args.outputDir)

    logging.info('Input URL is {}'.format(inputURL))
    logging.info('OutputDir is {}'.format(args.outputDir))

    num = len(filelist)
    for v in filelist: 
        outname=getDataFile(args.outputDir, inputURL, filelist[v])
        print(f"")
    logging.info('Finished moving {} files '.format(num))

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument('--inputURL', default=None, help='URL to retrieve data from', type=str)
    parser.add_argument('--outputDir', default=None, help='Destination directory', type=str)
    
    args = parser.parse_args()

    sys.exit(main(args))



#!/usr/bin/env python

# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

import os
import sys
import wget

import logging
import netCDF4 as nCDF
import zipfile
import glob
from rss_parser import Parser
from requests import get
from common.logging import LoggingUtil
from urllib import parse
from datetime import datetime, timezone

filelist = {'zeta_max': 'maxele.63.nc', 'swan_HS_max': 'swan_HS_max.63.nc', 'wind_max': 'maxwvel.63.nc', 'water_levels': 'fort.63.nc', 'inund_max': 'maxinundepth.63.nc'}
mode = 0o755

NHC_Url = "https://www.nhc.noaa.gov/gis-at.xml"
NHC_filelist = {'_pgn.': 'cone',
                '_pts.': 'points',
                '_lin.': 'track'}


def getDataFile(outdir, url, infilename, logger):
    logger.debug(f"getDataFile: args: outdir={outdir}  url={url}  infilename={infilename}")
    # init return value
    ret_val = None

    # Get infilename and download netcdf file
    logger.debug(f"About to wget - filename: {infilename}")
    try:
        url = url if url.endswith("/") else f"{url}/"

        ret_val = wget.download(parse.urljoin(url, infilename), os.path.join(outdir, infilename))
    except Exception as e:
        logger.error(e)

    return ret_val


def updateProjFile(filename, logger):
    prj_str = 'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'

    logger.info(f"updateProjFile: filename={filename}")
    if filename is not None:
        try:
            with open(filename, "w") as f:
                f.write(prj_str)
        except Exception as e:
            logger.warning(f"WARNING: Unable to update shapefile prj file for: {filename}")


def organizeNhcZips(shape_dir, out_file, logger):
    # unzip the NHC shapefiles and organize into a zip for each layer
    # logger.info(f"organizeNhcZips: shape_dir={shape_dir}  out_file={out_file}")
    # unzip the NHC shapefile zip
    with zipfile.ZipFile(os.path.join(shape_dir, out_file), 'r') as zip_ref:
        zip_ref.extractall(shape_dir)

    logger.info(f"Extracted NHC zipfile into {shape_dir}")

    # now - first remove the original zipfile
    os.remove(os.path.join(shape_dir, out_file))

    # go through each name and create an archive for each
    # for name in distinct_name_list:
    for key in NHC_filelist:
        # get list of files for this pattern
        file_list = glob.glob(f"{shape_dir}/*{key}*")
        logger.info(f"file_list: {file_list}")

        # modify the .prj file with better projection info
        proj_file = f'{file_list[0].split(".")[0]}.prj'
        logger.info(f"proj_file={proj_file}")
        updateProjFile(proj_file, logger)

        # set full path name for zipfile we are about to create
        full_path = f"{os.path.join(shape_dir, NHC_filelist[key])}.zip"
        zipObj = zipfile.ZipFile(full_path, 'w')

        # add each file to the zip archive
        for specific_name in file_list:
            logger.debug(f"adding file {specific_name} to zipfile: {full_path}")
            zipObj.write(specific_name, os.path.basename(specific_name))

        zipObj.close()

    return


def retrieveStormShapefiles(outputDir, storm_number, logger):
    logger.info(f"retrieveStormShapefiles: outputDir={outputDir}  storm_number={storm_number}")

    # pad storm number if single digit
    storm_str = str(storm_number)
    if (len(storm_str) < 2):
        storm_str = f"0{storm_str}"

    # get current year in UTC - needed to build search string for RSS feed titles
    now = datetime.now(timezone.utc)
    # shp_srch_str = f"[shp] - Hurricane {stormname.lower().capitalize()}"
    shp_srch_array = ["[shp] -", f"/AL{storm_str}{now.year}"]
    cone_srch_str = "Cone of Uncertainty"
    shape_dir = f"{outputDir}/shapefiles"

    # mkdir for shapefiles
    try:
        if not os.path.exists(shape_dir):
            os.makedirs(shape_dir)
    except Exception as e:
        logger.error(f"Error: Could not create shapefile output directory: {e}")
        return

    # retrieve the rss feed xml from the nhc
    rss_url = NHC_Url
    xml = None
    try:
        xml = get(rss_url)
    except Exception as e:
        logger.error(f"Could not get NHC RSS url: {rss_url}  error: {e}")

    if xml is None:
        logger.error(f"Nothing return for NHC RSS url: {rss_url}")
        return

    logger.debug("NHC RSS feed info returned:")
    logger.debug(xml.content)

    # parse the RSS feed content (basically just XML)
    try:
        parser = Parser(xml=xml.content)
        feed = parser.parse()
    except Exception as e:
        logger.error(f"Cannot parse NHC RSS Feed: {e}")
        # just give up on this and return
        return

    # Iteratively search through feed items
    download_url = None
    for item in feed.feed:
        # look for particular shapefiles that we are interested in
        # if they are not found - that most like means there in no active tropical storm
        # example title: <title>Advisory #027 Forecast [shp] - Hurricane Earl (AT1/AL062022)</title>
        # example description: <description>Forecast Track, Cone of Uncertainty, Watches/Warnings. Shapefile last updated Fri, 09 Sep 2022 14:37:42 GMT</description>
        # if ((shp_srch_str in item.title) and (cone_srch_str in item.description)):
        if ((all(x in item.title for x in shp_srch_array)) and (cone_srch_str in item.description)):
            # now get the download url for these shapefiles and organize in output dir
            download_url = item.link
            logger.info(f"Found download link for shapefiles: {download_url}")
            break

    if download_url is not None:
        # now wget the shapefile zip
        # example url: https://www.nhc.noaa.gov/gis/forecast/archive/al062022_5day_027.zip
        url_parts = parse.urlparse(download_url)
        path_parts = url_parts[2].rpartition('/')
        out_file = path_parts[2]

        try:

            ret_val = wget.download(download_url, os.path.join(shape_dir, out_file))
            if ret_val is None:
                # in this case the download failed
                # so log the error and return
                logger.error(f"Download of NHC shapefiles failed, url: {download_url}")
                return
        except Exception as e:
            logger.error(f"Exception occured during download of NHC shapefiles, url: {download_url}  error: {e}")

        logger.info(f"Downloaded NHC shapefile zip into: {os.path.join(shape_dir, out_file)}")
        # now unzip and organize into new zipfiles
        organizeNhcZips(shape_dir, out_file, logger)

        return


def main(args):
    '''    
    '''
    # logging.basicConfig(filename='log',format='%(asctime)s : %(levelname)s : %(funcName)s : %(module)s : %(name)s : %(message)s', level=logging.WARNING)
    # get the log level and directory from the environment
    log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
    log_path: str = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), str('logs')))

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

    if not args.isHurricane:
        logger.info("Need isHurricane flag on command line: --isHurricane <Stormnumber/NA>.")
        return 1

    try:
        if not os.path.exists(args.outputDir):
            os.makedirs(args.outputDir)
    except Exception as e:
        logger.error(f"Error: Could not create file output directory: {e}")

    logger.info('Input URL is {}'.format(inputURL))
    logger.info('OutputDir is {}'.format(args.outputDir))

    # if this is a tropical storm, stage storm track layers for subsequent storage in GeoServer
    try:
        storm_number = int(args.isHurricane)
        retrieveStormShapefiles(args.outputDir, storm_number, logger)
    except ValueError:
        logger.debug("This is not a storm run")

    error = False
    num = 0

    for key in filelist:
        logger.debug(f"Processing NetDCF file: {filelist[key]}")
        # grab the file
        outname = getDataFile(args.outputDir, inputURL, filelist[key], logger)

        # if there was a file gathered
        if outname is not None:
            logger.debug(f"Downloaded filename is: {outname}")
            # load the NetCDF file
            try:
                logger.debug(f"Checking validity of NetCDF file {outname}")
                f = nCDF.Dataset(outname)

                # each NetCDF dimension must have a non-zero value
                for dim in f.dimensions.values():
                    # is there a value
                    if dim.size == 0:
                        # log the error
                        logger.error(f'Error: NetCDF Dimension "{dim.name}" for file {key} ({filelist[key]}) is invalid.')
                        # declare failure
                        error = True
            except Exception as e:
                logger.error('Error checking validity of NetCDF file "{outname}": {e}')

            num = num + 1

        # else the file was not found
        else:
            # swan files are optional
            if not key.startswith("swan") and not key.startswith("inund"):
                # log the error
                logger.error(f'Error: NetCDF file {key} ({filelist[key]}) was not found.')

                # declare failure
                error = True

        # if there was an error, abort
        if error:
            # no need to continue
            logger.debug("Error is True - sys exiting with value of 1")
            sys.exit(1)

    logger.info('Finished moving {} files '.format(num))


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument('--inputURL', default=None, help='URL to retrieve data from', type=str)
    parser.add_argument('--outputDir', default=None, help='Destination directory', type=str)
    parser.add_argument('--isHurricane', default=None, help='Hurricane run flag', type=str)
    args = parser.parse_args()

    sys.exit(main(args))

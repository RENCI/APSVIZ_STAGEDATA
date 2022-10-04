#!/usr/bin/env python
# finalize_data.py --inputDir $stageDir/* --outputDir

import os
import sys
import tarfile
import shutil
import logging

from common.logging import LoggingUtil

mode = 0o755


def make_tarfile(ofilename, inputdir, logger):
    """
    Get everything under input_dir in a relative path scheme
    also return the number of member files as a check
    """
    with tarfile.open(ofilename, "w:gz") as tar:
        tar.add(inputdir, arcname=os.path.basename(inputdir))
    logger.info('Created a taf file with the name {}'.format(ofilename))
    with tarfile.open(ofilename) as archive:
        num = sum(1 for member in archive if member.isreg())
    return num


def main(in_args):
    """
    Simple processor to assemble into a tarball all the files that exists under the input --inputDir
    and move the tarball (and possibly expand) into the output directory --outputDir

    Updated 5/27/21 : Tarball no longer needed. This process will now just clean up all the data
    created for this model run
    """
    # logging.basicConfig(filename='log',format='%(asctime)s : %(levelname)s : %(funcName)s : %(module)s : %(name)s : %(message)s', level=logging.WARNING)
    # get the log level and directory from the environment
    log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
    log_path: str = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), str('logs')))

    # create the dir if it does not exist
    if not os.path.exists(log_path):
        os.mkdir(log_path)

    # create a logger
    logger = LoggingUtil.init_logging("APSVIZ.finalize_data", level=log_level, line_format='medium',
                                      log_file_path=log_path)

    # process args
    if not in_args.inputDir:
        print(f"Need inputDir on command line: --inputDir $stageDir")
        return 1

    input_dir = in_args.inputDir.strip()

    if not in_args.outputDir:
        logger.error("Need output directory on command line: --output <outputdir>.")
        return 1

    # Not needed anymore
    """
    if not os.path.exists(args.outputDir):
        logger.error("Create Output dir {}".format(args.outputDir))
        os.makedirs(args.outputDir)
    """

    logger.info('Input URL is {}'.format(input_dir))
    logger.info('OutputDir is {}'.format(in_args.outputDir))

    # inputDir looks something like this: /data/2900-2021052612-namforecast/final
    # want to remove dir from one level up - i.e. here : /data/2900-2021052612-namforecast
    dir_to_remove = input_dir
    dir_parts = input_dir.split('/')
    if len(dir_parts) > 2:
        dir_to_remove = f'/{dir_parts[1]}/{dir_parts[2]}'

    # check to see if the directory exists
    if not os.path.exists(dir_to_remove):
        logger.error("Missing Input dir {}".format(dir_to_remove))
        return 1

    logger.info('Removing dir: {}'.format(dir_to_remove))
    try:
        if os.path.isdir(dir_to_remove):
            shutil.rmtree(dir_to_remove)
    except OSError as e:
        print("Error: %s : %s" % (dir_to_remove, e.strerror))

    # if args.externalDir not None:
    #    utilities.log.info('An external dir was specified. Checking status is the job of the caller {}'.args.externalDir)

    # Construct local tarball
    ''' Don't need this code anymore
    logger.info('Try to construct a tarfile archive at {}'.format(inputDir))
    tarname = '_'.join([args.tarMeta,'archive.tar.gz'])
    num_files = make_tarfile(tarname, args.inputDir, logger)
    logger.info('Number of files archived to tar is {}'.format(num_files))

    if args.outputDir is not None:
        output_tarname='/'.join([args.outputDir,tarname])
        shutil.move(tarname, output_tarname)
        logger.info('Tar file moved to distination name {}'.format(output_tarname))
        # (optionally) unpack the tar ball  in the destination dir
        out_tar = tarfile.open(output_tarname)
        out_tar.extractall(args.outputDir) # specify which folder to extract to
        out_tar.close()
        logger.info('Unpacked destination tar file into {}'.format(args.outputDir))

    if args.externalDir is not None:
        # We want to pass back the full directory hierarchy instead of the tar file back to the caller at the indicated location
        logger.info('Send tarfile back to the caller at {}'.args.externalDir)
        bldcmd = 'scp -r -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '+args.inputDir+' '+args.externalDir
        logger.info('Execute cmd: {}'.format(bldcmd))
        os.system(bldcmd) 
        logger.info('Hierarchy of file sent back')
    '''

# Still need a password for this top work

    logger.info('finalize_data is finished')


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument('--inputDir', default=None, help='inputDir to retrieve data from', type=str)
    parser.add_argument('--outputDir', default=None, help='Destination directory', type=str)
    parser.add_argument('--externalDir', default=None, help='External to RENCI destination directory syntax: user@host://dir', type=str)
    parser.add_argument('--tarMeta', default='test', help='Tar file metadata (metadata_archive.gz)', type=str)
    
    args = parser.parse_args()

    sys.exit(main(args))

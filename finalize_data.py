#!/usr/bin/env python
#finalize_data.py --inputDir $stageDir/* --outputDir

import os, sys, wget
import tarfile
import shutil
import logging

mode = 0o755

def make_tarfile(ofilename, inputdir):
    '''
    Get everything under input_dir in a relative path scheme
    also return the number of member files as a check
    '''
    with tarfile.open(ofilename, "w:gz") as tar:
        tar.add(inputdir, arcname=os.path.basename(inputdir))
    logging.info('Created a taf file with the name {}'.format(ofilename))
    with tarfile.open(ofilename) as archive:
        num = sum(1 for member in archive if member.isreg())
    return num

def main(args):
    '''    
    Simple processor to assemble into a tarball all the files that exists under the input --inputDir
    and move the tarball (and possibly expand) into the output directory --outputDir

    For now, the outputDir must be a local file system.
    '''
    logging.basicConfig(filename='log',format='%(asctime)s : %(levelname)s : %(funcName)s : %(module)s : %(name)s : %(message)s', level=logging.WARNING)

    # process args
    if not args.inputDir:
        print(f"Need inputDir on command line: --inputDir $stageDir")
        return 1
    inputDir = args.inputDir.strip()

    if not args.outputDir:
        logging.error("Need output directory on command line: --output <outputdir>.")
        return 1

    if not os.path.exists(args.inputDir):
        logging.error("Missing Input dir {}".format(args.inputDir))
        return 1

    if not os.path.exists(args.outputDir):
        logging.error("Create Output dir {}".format(args.outputDir))
        os.makedirs(args.outputDir)

    logging.info('Input URL is {}'.format(inputDir))
    logging.info('OutputDir is {}'.format(args.outputDir))

    #if args.externalDir not None:
    #    utilities.log.info('An external dir was specified. Checking status is the job of the caller {}'.args.externalDir)

    # Construct local tarball
    logging.info('Try to construct a tarfile archive at {}'.format(inputDir))
    tarname = '_'.join([args.tarMeta,'archive.tar.gz'])
    num_files = make_tarfile(tarname, args.inputDir)
    logging.info('Number of files archived to tar is {}'.format(num_files))

    if args.outputDir is not None:
        output_tarname='/'.join([args.outputDir,tarname])
        shutil.move(tarname, output_tarname)
        logging.info('Tar file moved to distination name {}'.format(output_tarname))
        # (optionally) unpack the tar ball  in the destination dir
        out_tar = tarfile.open(output_tarname)
        out_tar.extractall(args.outputDir) # specify which folder to extract to
        out_tar.close()
        logging.info('Unpacked destination tar file into {}'.format(args.outputDir))

    if args.externalDir is not None:
        # We want to pass back the full directory hierarchy instead of the tar file back to the caller at the indicated location
        utilities.log.info('Send tarfile back to the caller at {}'.args.externalDir)
        bldcmd = 'scp -r -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '+args.inputDir+' '+args.externalDir
        utilities.log.info('Execute cmd: {}'.format(bldcmd))
        os.system(bldcmd) 
        utilities.log.info('Hierarchy of file sent back')

# Still need a password for this top work

    logging.info('finalize_data is finished')

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument('--inputDir', default=None, help='inputDir to retrieve data from', type=str)
    parser.add_argument('--outputDir', default=None, help='Destination directory', type=str)
    parser.add_argument('--externalDir', default=None, help='External to RENCI destination directory syntax: user@host://dir', type=str)
    parser.add_argument('--tarMeta', default='test', help='Tar file metadata (metadata_archive.gz)', type=str)
    
    args = parser.parse_args()

    sys.exit(main(args))



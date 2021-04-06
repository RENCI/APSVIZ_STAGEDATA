import os, sys
import fnmatch
import logging
import psycopg2
import urllib.parse

from geo.Geoserver import Geoserver
from geoserver.catalog import Catalog
from common.logging import LoggingUtil
from subprocess import call

class asgsDB:

    def __init__(self, logger):
        self.conn = None
        self.logger = logger

        user = os.getenv('ASGS_DB_USERNAME', 'user').strip()
        pswd = os.getenv('ASGS_DB_PASSWORD', 'password').strip()
        host = os.getenv('ASGS_DB_HOST', 'host').strip()
        port = os.getenv('ASGS_DB_PORT', '5432').strip()
        db_name = os.getenv('ASGS_DB_DATABASE', 'asgs').strip()

        try:
            # connect to asgs database
            conn_str = f'host={host} port={port} dbname={db_name} user={user} password={pswd}'
            logger.debug("Connecting to ASGS DB - coonection string={conn_str}")

            self.conn = psycopg2.connect(conn_str)
            self.conn.set_session(autocommit=True)
        except:
            e = sys.exc_info()[0]
            self.logger.error(f"FAILURE - Cannot connect to ASGS_DB. error {e}")

    def __del__(self):
        if (self.conn):
            self.conn.close()

    # given instance id - save geoserver url (to access this mbtiles layer) in the asgs database
    def saveImageURL(self, instanceId, name, url):
        self.logger.info(f'Updating DB record - instance id: {instanceId} with url: {url}')

        # format of mbtiles is ex: maxele.63.0.9.mbtiles
        # final key value will be in this format image.maxele.63.0.9
        key_name = "image." + os.path.splitext(name)[0]
        key_value = url

        try:
            cursor = self.conn.cursor()

            sql_stmt = 'INSERT INTO "ASGS_Mon_config_item" (key, value, instance_id) VALUES(%s, %s, %s)'
            params = [f"'{key_name}'", f"'{key_value}'", instanceId]
            self.logger.debug(f"sql statement is: {sql_stmt} params are: {params}")

            cursor.execute(sql_stmt, params)
        except:
             e = sys.exc_info()[0]
             self.logger.error(f"FAILURE - Cannot update ASGS_DB. error {e}")

    def getRunMetadata(self, instanceId):
        self.logger.debug(f'Retrieving DB record metadata - instance id: {instanceId}')

        try:
            cursor = self.conn.cursor()

            sql_stmt = 'SELECT key FROM "ASGS_Mon_config_item" key, value, instance_id WHERE instance_id=%s'
            params = [f"'{key_name}'", f"'{key_value}'", instanceId]
            self.logger.debug(f"sql statement is: {sql_stmt} params are: {params}")

            cursor.execute(sql_stmt, params)
        except:
             e = sys.exc_info()[0]
             self.logger.error(f"FAILURE - Cannot update ASGS_DB. error {e}")



 # create a new workspace in geoserver if it does not already exist
def add_workspace(logger, geo, worksp):
    if (geo.get_workspace(worksp) is None):
        geo.create_workspace(workspace=worksp)


# add a coverage store to geoserver for each .mbtiles found in the staging dir
def add_mbtiles_coveragestores(logger, geo, url, instance_id, worksp, mbtiles_path):
    # format of mbtiles is ex: maxele.63.0.9.mbtiles
    # pull out meaningful pieces of file name
    # get all files in mbtiles dir and loop through
    for file in fnmatch.filter(os.listdir(mbtiles_path), '*.mbtiles'):
        file_path = f"{mbtiles_path}/{file}"
        logger.debug(f"add_mbtiles_coveragestores: file={file_path}")
        layer_name = str(instance_id) + "_" + os.path.splitext(file)[0]
        logger.info(f'Adding layer: {layer_name} into workspace: {worksp}')

        # create coverage store and associate with .mbtiles file
        # also creates layer
        fmt = f"mbtiles?configure=first&coverageName={layer_name}"
        ret = geo.create_coveragestore(lyr_name=layer_name,
                                       path=file_path,
                                       workspace=worksp,
                                       file_type=fmt,
                                       content_type='application/vnd.sqlite3')
        logger.debug(f"Attempted to add coverage store, file path: {file_path}  return value: {ret}")

        # update DB with url of layer for access from website NEED INSTANCE ID for this
        layer_url = f'{url}rest/workspaces/{worksp}/coveragestores/{layer_name}.json'
        logger.debug(f"Adding coverage store to DB, instanceId: {instance_id} coveragestore url: {layer_url}")
        asgsdb = asgsDB(logger)
        asgsdb.saveImageURL(instance_id, file, layer_url)


# add a datastore in geoserver for the stationProps.csv file
def add_props_datastore(logger, geo, instance_id, worksp, final_path):
    stations_filename = "stationProps.csv"
    insets_path = f"{final_path}/insets/{stations_filename}"
    store_name = str(instance_id) + "_station_props"
    ret = geo.create_datastore(name=store_name, path=insets_path, workspace=worksp)
    logger.debug(f"Attempted to add data store, file path: {insets_path}  return value: {ret}")


# copy all .png files to the geoserver host to serve them from there
def copy_pngs(logger, geoserver_host, geoserver_vm_userid, geoserver_proj_path, instance_id, final_path):

    from_path = f"{final_path}/insets/"
    to_path = f"{geoserver_vm_userid}@{geoserver_host}:{geoserver_proj_path}/{instance_id}/"

    # first create new directory if not already existing
    new_dir = f"{geoserver_proj_path}/{instance_id}"
    logger.debug(f"copy_pngs: Creating to path directory: {new_dir}")
    mkdir_cmd = f'ssh {geoserver_vm_userid}@{geoserver_host} "mkdir -p {new_dir}"'
    logger.debug(f"copy_pngs: mkdir_cmd.split={mkdir_cmd}.split()")
    call(mkdir_cmd.split())

    for file in fnmatch.filter(os.listdir(from_path), '*.png'):
        from_file_path = from_path + file
        to_file_path = to_path + file
        logger.debug(f"Copying .png file from: {from_file_path}  to: {to_file_path}")
        scp_cmd = f'scp -r -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" {from_file_path} {to_file_path}'
        call(scp_cmd.split())


# given an instance id and an input dir (where to find mbtiles)
# add the mbtiles to the specified GeoServer (configured with env vars)
# then update the specified DB with the access urls (configured with env vars)

def main(args):
    # logging.basicConfig(filename='stage-data-load-images.log',format='%(asctime)s : %(levelname)s : %(funcName)s : %(module)s : %(name)s : %(message)s', level=logging.DEBUG)

    # get the log level and directory from the environment
    log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
    log_path: str = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))

    # create the dir if it does not exist
    if not os.path.exists(log_path):
        os.mkdir(log_path)

    # create a logger
    logger = LoggingUtil.init_logging("APSVIZ.load-geoserver-images", level=log_level, line_format='medium',
                                      log_file_path=log_path)

    # process args
    if not args.instanceId:
        print("Need instance id on command line: --instanceId <instanceid>")
        return 1
    instance_id = args.instanceId.strip()

    # collect needed info from env vars
    user = os.getenv('GEOSERVER_USER', 'user').strip()
    pswd = os.environ.get('GEOSERVER_PASSWORD', 'password').strip()
    url = os.environ.get('GEOSERVER_URL', 'url').strip()
    worksp = os.environ.get('GEOSERVER_WORKSPACE', 'ADCIRC_2021').strip()
    geoserver_host = os.environ.get('GEOSERVER_HOST', 'host.here.org').strip()
    geoserver_vm_userid = os.environ.get('GEOSERVER_VM_USER', 'user').strip()
    geoserver_proj_path = os.environ.get('GEOSERVER_PROJ_PATH', '/projects').strip()
    logger.debug(f"Retrieved GeoServer env vars - url: {url}, workspace: {worksp}")

    logger.info(f"Connecting to GeoServer at host: {url}")
    # create a GeoServer connection
    geo = Geoserver(url, username=user, password=pswd)

    # create a new workspace in geoserver if it does not already exist
    add_workspace(logger, geo, worksp)

    # final dir path needs to be well defined
    # dir structure looks like this: /data/<instance id>/mbtiles/<parameter name>.<zoom level>.mbtiles
    final_path = "/data/" + instance_id
    mbtiles_path = final_path + "/mbtiles"

    # add a coverage store to geoserver for each .mbtiles found in the staging dir
    add_mbtiles_coveragestores(logger, geo, url, instance_id, worksp, mbtiles_path)

    # now put NOAA OBS .csv file into geoserver
    #add_props_datastore(logger, geo, instance_id, worksp, final_path)

    # finally copy all .png files to the geoserver host to serve them from there
    copy_pngs(logger, geoserver_host, geoserver_vm_userid, geoserver_proj_path, instance_id, final_path)



if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument('--instanceId', default=None, help='instance id of db entry for this model run', type=str)

    args = parser.parse_args()

    sys.exit(main(args))

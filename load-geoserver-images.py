import os, sys
import fnmatch
import logging
import psycopg2
import csv

from geo.Geoserver import Geoserver
from geoserver.catalog import Catalog
from common.logging import LoggingUtil
from urllib.parse import urlparse
from terria_catalog import TerriaCatalog

class asgsDB:

    # dbname looks like this: 'asgs_dashboard'
    # instance_id looks like this: '2744-2021050618-namforecast'
    def __init__(self, logger, dbname, instance_id):
        self.conn = None
        self.logger = logger

        self.user = os.getenv('ASGS_DB_USERNAME', 'user').strip()
        self.pswd = os.getenv('ASGS_DB_PASSWORD', 'password').strip()
        self.host = os.getenv('ASGS_DB_HOST', 'host').strip()
        self.port = os.getenv('ASGS_DB_PORT', '5432').strip()
        # self.db_name = os.getenv('ASGS_DB_DATABASE', 'asgs').strip()
        self.db_name = dbname

        # save whole Id
        self.instanceId = instance_id
        self.uid = instance_id
        self.instance = instance_id

        # also save separate parts i.e. '2744' and '2021050618-namforecast'
        parts = instance_id.split("-", 1)
        if (len(parts) > 1):
            self.instance = parts[0]
            self.uid = parts[1]

        try:
            # connect to asgs database
            conn_str = f'host={self.host} port={self.port} dbname={self.db_name} user={self.user} password={self.pswd}'

            self.conn = psycopg2.connect(conn_str)
            self.conn.set_session(autocommit=True)
            self.cursor = self.conn.cursor()
        except:
            e = sys.exc_info()[0]
            self.logger.error(f"FAILURE - Cannot connect to ASGS_DB. error {e}")

    def __del__(self):
        """
            close up the DB
            :return:
        """
        try:
            if self.cursor is not None:
                self.cursor.close()
            if self.conn is not None:
                self.conn.close()
        except Exception as e:
            self.logger.error(f'Error detected closing cursor or connection. {e}')
            sys.exc_info()[0]

    def get_user(self):
        return self.user

    def get_password(self):
        return self.pswd

    def get_host(self):
        return self.host

    def get_port(self):
        return self.port

    def get_dbname(self):
        return self.db_name


    # given instance id - save geoserver url (to access this mbtiles layer) in the asgs database
    def saveImageURL(self, name, url):
        self.logger.info(f'Updating DB record - instance id: {self.instance}  uid: {self.uid} with url: {url}')

        # format of mbtiles is ex: maxele.63.0.9.mbtiles
        # final key value will be in this format image.maxele.63.0.9
        key_name = "image." + os.path.splitext(name)[0]
        key_value = url

        try:
            sql_stmt = 'INSERT INTO "ASGS_Mon_config_item" (key, value, instance_id, uid) VALUES(%s, %s, %s, %s)'
            params = [f"{key_name}", f"{key_value}", self.instance, f"{self.uid}"]
            self.logger.debug(f"sql statement is: {sql_stmt} params are: {params}")

            self.cursor.execute(sql_stmt, params)
        except:
            e = sys.exc_info()[0]
            self.logger.error(f"FAILURE - Cannot update ASGS_DB. error {e}")

    # need to retrieve some values - related to this run - from the ASGS DB
    # currently: Date, Cycle, Storm Name (if any), and Advisory (if any)
    # if more are needed, add to metadata_dict
    def getRunMetadata(self):
        metadata_dict = {
            'currentdate': '',
            'currentcycle': '',
            'advisory': '',
            'forcing.stormname': '',
            'asgs.enstorm': '',
            'ADCIRCgrid': ''
        }
        self.logger.info(f'Retrieving DB record metadata - instance id: {self.instance} uid: {self.uid}')

        try:
            for key in metadata_dict.keys():
                sql_stmt = 'SELECT value FROM "ASGS_Mon_config_item" WHERE instance_id=%s AND uid=%s AND key=%s'
                params = [self.instance, self.uid, key]
                self.logger.debug(f"sql statement is: {sql_stmt} params are: {params}")
                self.cursor.execute(sql_stmt, params)
                ret = self.cursor.fetchone()
                if ret:
                    self.logger.debug(f"value returned is: {ret}")
                    metadata_dict[key] = ret[0]
        except:
             e = sys.exc_info()[0]
             self.logger.error(f"FAILURE - Cannot retrieve run properties metadata from ASGS_DB. error {e}")
        finally:
            return metadata_dict

    # find the stationProps.csv file and insert the contents
    # into the adcirc_obs db of the ASGS postgres instance
    def insert_station_props(self, logger, geo, worksp, csv_file_path, geoserver_host):
        # where to find the stationProps.csv file
        logger.info(f"Saving {csv_file_path} to DB")
        logger.debug(f"DB name is: {self.get_dbname()}")

        # need to remove the .edc from the geoserver_host for now
        host = geoserver_host.replace('.edc', '')

        # open the stationProps.csv file and save in db
        # must create the_geom from lat, lon provided in csv file
        # also add to instance id column
        # and finally, create an url where the obs chart for each station can be accessed
        #try: catch this exception in calling program instead
        with open(csv_file_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row.
            for row in reader:
                logger.debug(f"opened csv file - saving this row to db: {row}")
                png_url = f"https://{host}/obs_pngs/{self.instanceId}/{row[6]}"
                sql_stmt = "INSERT INTO stations (stationid, stationname, state, lat, lon, node, filename, the_geom, instance_id, imageurl) VALUES (%s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s),4326), %s, %s)"
                params = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[4], row[3], self.instanceId, png_url]
                logger.debug(f"sql_stmt: {sql_stmt} params: {sql_stmt}")
                self.cursor.execute(sql_stmt, params)

        self.conn.commit()
        #except:
            #e = sys.exc_info()[0]
            #self.logger.error(f"FAILURE - Cannot save run properties in ASGS_DB. error {e}")




 # create a new workspace in geoserver if it does not already exist
def add_workspace(logger, geo, worksp):
    logger.info(f"workspace: {worksp}")
    if (geo.get_workspace(worksp) is None):
        geo.create_workspace(workspace=worksp)


# tweak the layer title to make it more readable in Terria Map
def update_layer_title(logger, geo, instance_id, worksp, layer_name):
    logger.info(f"instance_id: {instance_id} layer_name: {layer_name}")
    run_date = ''
    # first get metadata from this model run
    db_name = os.getenv('ASGS_DB_DATABASE', 'asgs').strip()
    asgsdb = asgsDB(logger, db_name, instance_id)
    meta_dict = asgsdb.getRunMetadata()
    raw_date = meta_dict['currentdate']
    if raw_date:
        # raw date format is YYMMDD
        date_list = [raw_date[i:i+2] for i in range(0, len(raw_date), 2)]
        if len(date_list) == 3:
            run_date = f"{date_list[1]}-{date_list[2]}-20{date_list[0]}"

    title = "N/A"
    if (meta_dict['forcing.stormname'] == 'NA'):
        title = f"Date: {run_date} Cycle: {meta_dict['currentcycle']} Storm Name: {meta_dict['asgs.enstorm']} ADCIRC Grid: {meta_dict['ADCIRCgrid']} ({layer_name.split('_')[1]})"
    else:
        title = f"Date: {run_date} Cycle: {meta_dict['currentcycle']} Storm Name: {meta_dict['forcing.stormname']}:{meta_dict['asgs.enstorm']} Advisory:{meta_dict['advisory']} ADCIRC Grid: {meta_dict['ADCIRCgrid']} ({layer_name.split('_')[1]})"
    logger.debug(f"setting this coverage: {layer_name} to {title}")

    geo.set_coverage_title(worksp, layer_name, layer_name, title)

    return title


# add a coverage store to geoserver for each .mbtiles found in the staging dir
def add_mbtiles_coveragestores(logger, geo, url, instance_id, worksp, mbtiles_path, layergrp):
    # format of mbtiles is ex: maxele.63.0.9.mbtiles
    # pull out meaningful pieces of file name
    # get all files in mbtiles dir and loop through
    logger.info(f"instance_id: {instance_id} mbtiles_path: {mbtiles_path}")
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
        if (ret is None):  # coverage store successfully created

            # now we just need to tweak the layer title to make it more
            # readable in Terria Map
            title = update_layer_title(logger, geo, instance_id, worksp, layer_name)

            # update DB with url of layer for access from website NEED INSTANCE ID for this
            layer_url = f'{url}rest/workspaces/{worksp}/coveragestores/{layer_name}.json'
            logger.debug(f"Adding coverage store to DB, instanceId: {instance_id} coveragestore url: {layer_url}")
            db_name = os.getenv('ASGS_DB_DATABASE', 'asgs').strip()
            asgsdb = asgsDB(logger, db_name, instance_id)
            asgsdb.saveImageURL(file, layer_url)

            # add this layer to the wms layer group dict
            full_layername = f"{worksp}:{layer_name}"
            layergrp["wms"].append({"title": title, "layername": full_layername})

    return layergrp



# add a datastore in geoserver for the stationProps.csv file
# as of 4/8/21 this feature is broken in GeoServer so going to
# add a DB datastore for this data
#def add_props_datastore(logger, geo, instance_id, worksp, final_path):
    #stations_filename = "stationProps.csv"
    #insets_path = f"{final_path}/insets/{stations_filename}"
    #store_name = str(instance_id) + "_station_props"
    #ret = geo.create_datastore(name=store_name, path=insets_path, workspace=worksp)
    #logger.debug(f"Attempted to add data store, file path: {insets_path}  return value: {ret}")


# add a datastore in geoserver for the stationProps.csv file
def add_props_datastore(logger, geo, instance_id, worksp, final_path, geoserver_host, layergrp):
    logger.info(f"Adding the station properties datastore for instance id: {instance_id}")
    # set up paths and datastore name
    # TODO put these in ENVs
    stations_filename = "stationProps.csv"
    csv_file_path = f"{final_path}/insets/{stations_filename}"
    store_name = str(instance_id) + "_station_props"
    dbname = "adcirc_obs"
    table_name = "stations"

    logger.debug(f"csv_file_path: {csv_file_path} store name: {store_name}")

    # get asgs db connection
    asgs_obsdb = asgsDB(logger, dbname, instance_id)
    # save stationProps file to db
    try: # make sure this completes before moving on - observations may not exist for this grid
        asgs_obsdb.insert_station_props(logger, geo, worksp, csv_file_path, geoserver_host)
    except (IOError, OSError):
        e = sys.exc_info()[0]
        logger.warning(f"WARNING - Cannot save run properties in ASGS_DB. Error: {e}")
        return layergrp

    # create this layer in geoserver
    #geo.create_featurestore(store_name, workspace=worksp, db=dbname, host=asgs_obsdb.get_host(), port=asgs_obsdb.get_port(), schema=table_name,
                            #pg_user=asgs_obsdb.get_user(), pg_password=asgs_obsdb.get_password(), overwrite=False)
    # ... using pre-defined postgresql JNDI feature store in Geoserver
    ret = geo.create_jndi_featurestore(store_name, worksp, overwrite=False)
    if ret is None: # sucessful

        # now publish this layer with an SQL filter based on instance_id
        sql = f"select * from stations where instance_id='{instance_id}'"
        name = f"{instance_id}_station_properies_view"

        # TODO probably need to update this name - 5/21/21 - okay updated ...
        #  but maybe need to make this a little less messy
        db_name = os.getenv('ASGS_DB_DATABASE', 'asgs').strip()
        asgsdb = asgsDB(logger, db_name, instance_id)
        meta_dict = asgsdb.getRunMetadata()
        raw_date = meta_dict['currentdate']
        if raw_date:
            # raw date format is YYMMDD
            date_list = [raw_date[i:i + 2] for i in range(0, len(raw_date), 2)]
            if len(date_list) == 3:
                run_date = f"{date_list[1]}-{date_list[2]}-20{date_list[0]}"
        if (meta_dict['forcing.stormname'] == 'NA'):
            title = f"NOAA Observations - Date: {run_date} Cycle: {meta_dict['currentcycle']} Storm Name: {meta_dict['asgs.enstorm']} ADCIRC Grid: {meta_dict['ADCIRCgrid']}"
        else:
            title = f"NOAA Observations - Date: {run_date} Cycle: {meta_dict['currentcycle']} Storm Name: {meta_dict['forcing.stormname']}:{meta_dict['asgs.enstorm']} Advisory:{meta_dict['advisory']} ADCIRC Grid: {meta_dict['ADCIRCgrid']}"
        geo.publish_featurestore_sqlview(name, title, store_name, sql, key_column='gid', geom_name='the_geom', geom_type='Geometry', workspace=worksp)

        # add this layer to the wfs layer group dict
        full_layername = f"{worksp}:{name}"
        layergrp["wfs"].append({"title": title, "layername": full_layername})

    return layergrp


# copy all .png files to the geoserver host to serve them from there
def copy_pngs(logger, geoserver_host, geoserver_vm_userid, geoserver_proj_path, instance_id, final_path):

    from_path = f"{final_path}/insets/"
    to_path = f"{geoserver_vm_userid}@{geoserver_host}:{geoserver_proj_path}/{instance_id}/"
    logger.info(f"Copying insets png files from: {from_path} to: {to_path}")
    # first create new directory if not already existing
    new_dir = f"{geoserver_proj_path}/{instance_id}"
    logger.debug(f"copy_pngs: Creating to path directory: {new_dir}")
    mkdir_cmd = f'ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no {geoserver_vm_userid}@{geoserver_host} "mkdir -p {new_dir}"'
    logger.debug(f"copy_pngs: mkdir_cmd={mkdir_cmd}")
    os.system(mkdir_cmd)

    # now go through any .png files in the insets dir, if it exists
    if (os.path.isdir(from_path)):
        for file in fnmatch.filter(os.listdir(from_path), '*.png'):
            from_file_path = from_path + file
            to_file_path = to_path + file
            logger.debug(f"Copying .png file from: {from_file_path}  to: {to_file_path}")
            scp_cmd = f'scp -r -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no {from_file_path} {to_file_path}'
            os.system(scp_cmd)

    # also now pick up legend .png files in the tiff directory
    from_path = f"{final_path}/tiff/"
    if (os.path.isdir(from_path)):
        for file in fnmatch.filter(os.listdir(from_path), '*.png'):
            from_file_path = from_path + file
            to_file_path = to_path + file
            logger.debug(f"Copying .png file from: {from_file_path}  to: {to_file_path}")
            scp_cmd = f'scp -r -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no {from_file_path} {to_file_path}'
            os.system(scp_cmd)


# given an instance id and an input dir (where to find mbtiles)
# add the mbtiles to the specified GeoServer (configured with env vars)
# then update the specified DB with the access urls (configured with env vars)

def main(args):

    # define dict to hold all of the layers created in this run
    # arrays contain sub-dicts like this: {"title": "", "layername": ""}
    layergrp = {
                 "wms": [],
                 "wfs": []
               }

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
    geoserver_vm_userid = os.environ.get('SSH_USERNAME', 'user').strip()
    geoserver_proj_path = os.environ.get('GEOSERVER_PROJ_PATH', '/projects').strip()
    logger.debug(f"Retrieved GeoServer env vars - url: {url} workspace: {worksp} geoserver_host: {geoserver_host} geoserver_proj_path: {geoserver_proj_path}")

    logger.info(f"Connecting to GeoServer at host: {url}")
    # create a GeoServer connection
    geo = Geoserver(url, username=user, password=pswd)

    # create a new workspace in geoserver if it does not already exist
    add_workspace(logger, geo, worksp)

    # final dir path needs to be well defined
    # dir structure looks like this: /data/<instance id>/mbtiles/<parameter name>.<zoom level>.mbtiles
    final_path = "/data/" + instance_id + "/final"
    mbtiles_path = final_path + "/mbtiles"

    # eventually use this to put tiffs into GeoServer as well
    #tiff_path = final_path + "/tiff"

    # add a coverage store to geoserver for each .mbtiles found in the staging dir
    new_layergrp = add_mbtiles_coveragestores(logger, geo, url, instance_id, worksp, mbtiles_path, layergrp)

    # now put NOAA OBS .csv file into geoserver
    final_layergrp = add_props_datastore(logger, geo, instance_id, worksp, final_path, geoserver_host, new_layergrp)

    # finally copy all .png files to the geoserver host to serve them from there
    copy_pngs(logger, geoserver_host, geoserver_vm_userid, geoserver_proj_path, instance_id, final_path)

    # update TerriaMap data catalog
    # build url to find existing apsviz.json file
    url_parts = urlparse(url)
    cat_url = f"{url_parts.scheme}://{url_parts.hostname}/obs_pngs/apsviz.json"
    tc = TerriaCatalog(cat_url, geoserver_host, geoserver_vm_userid, pswd)
    tc.update(final_layergrp)


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument('--instanceId', default=None, help='instance id of db entry for this model run', type=str)

    args = parser.parse_args()

    sys.exit(main(args))

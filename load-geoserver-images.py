import os, sys
import fnmatch
import logging
import psycopg2
from geo.Geoserver import Geoserver
from geoserver.catalog import Catalog


def asgsDB_connect():
    conn = None
    cursor = None

    user = os.getenv('ASGS_DB_USERNAME', 'user').strip()
    pswd = os.getenv('ASGS_DB_PASSWORD', 'password').strip()
    host = os.getenv('ASGS_DB_HOST', '172.25.16.10').strip()
    port = os.getenv('ASGS_DB_PORT', '5432').strip()
    db_name = os.getenv('ASGS_DB_DATABASE', 'asgs').strip()

    try:
        conn_str = f'host={host} port={port} dbname={db_name} user={user} password={pswd}'

        conn = psycopg2.connect(conn_str)
        conn.set_session(autocommit=True)
    except:
        e = sys.exc_info()[0]
        logging.error(f"FAILURE - Cannot connect to ASGS_DB. error {e}")
    finally:
        return conn


def asgsDB_close(conn):

    if (conn):
        conn.close()

# Add the geoserver url for this image to the DB
def asgsDB_update(instanceId, name, url):
    logging.info(f'Updating DB record - instance id: {instanceId} with url: {url}')
    conn = asgsDB_connect()

    # format of mbtiles is ex: maxele.63.0.9.mbtiles
    # final key value will be in this format image.maxele.63.0.9
    key_name = "image." + os.path.splitext(name)[0]
    key_value = url

    try:
        cursor = conn.cursor()

        sql_stmt = 'INSERT INTO "ASGS_Mon_config_item" (key, value, instance_id) VALUES(%s, %s, %s)'
        params = [f"'{key_name}'", f"'{key_value}'", instanceId]
        logging.debug(f"sql statement is: {sql_stmt} params are: {params}")

        cursor.execute(sql_stmt, params)
    except:
        e = sys.exc_info()[0]
        logging.error(f"FAILURE - Cannot update ASGS_DB. error {e}")
    finally:
        asgsDB_close(conn)

# given an instance id and an input dir (where to find mbtiles)
# add the mbtiles to the specified GeoServer (configured with env vars)
# then update the specified DB with the access urls (configured with env vars)

def main(args):
    logging.basicConfig(filename='stage-data-load-images.log',format='%(asctime)s : %(levelname)s : %(funcName)s : %(module)s : %(name)s : %(message)s', level=logging.DEBUG)

    # process args
    if not args.instanceId:
        print("Need instance id on command line: --instanceId <instanceid>")
        return 1
    instance_id = args.instanceId.strip()

    user = os.getenv('GEOSERVER_USER', 'user').strip()
    pswd = os.environ.get('GEOSERVER_PASSWORD', 'password').strip()
    url = os.environ.get('GEOSERVER_URL', 'url').strip()
    worksp = os.environ.get('GEOSERVER_WORKSPACE', 'ADCIRC_2021').strip()

    logging.info(f"Connecting to GeoServer at host: {url}")
    # create a GeoServer connection
    geo = Geoserver(url, username=user, password=pswd)

    # create a new workspace
    if (geo.get_workspace(worksp) is None):
        geo.create_workspace(workspace=worksp)

    # final dir path needs to be well defined
    # dir structure looks like this: /data/<instance id>/mbtiles/<parameter name>.<zoom level>.mbtiles
    final_path = "/data/" + instance_id
    mbtiles_path = final_path + "/mbtiles"

    # temporary file set to test with
    #tile_set = {
        #"maxele.63.0.9",
        #"maxwvel.63.0.9",
        #"swan_HS_max.63.0.9",
        #"maxele.63.10.10",
        #"maxwvel.63.10.10",
        #"swan_HS_max.63.10.10",
        #"maxele.63.1.11",
        #"maxwvel.63.11.11",
        #"swan_HS_max.63.11.11",
        #"maxele.63.12.12",
        #"maxwvel.63.12.12",
        #"swan_HS_max.63.12.12"
    #}

    # format of mbtiles is ex: maxele.63.0.9.mbtiles
    # pull out meaningful pieces of file name
    # get all files in mbtiles dir and loop through
    for file in fnmatch.filter(os.listdir(mbtiles_path), '*.mbtiles'):
        file_path = f"{mbtiles_path}/{file}"
        layer_name = str(instance_id) + "_" + os.path.splitext(file)[0]
        logging.info(f'Adding layer: {layer_name} into workspace: {worksp}')

        # create datastore and associate with .mbtiles file
        ret = geo.create_coveragestore(lyr_name=layer_name, path=file_path, workspace=worksp, file_type='mbtiles')
        logging.debug(f"Attempted to add coverage store, file path: {file_path}  return value: {ret}")

        # update DB with url of layer for access from website NEED INSTANCE ID for this
        layer_url = f'{url}rest/workspaces/{worksp}/coveragestores/{layer_name}.json'
        asgsDB_update(instance_id, file, layer_url)


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument('--instanceId', default=None, help='instance id of db entry for this model run', type=str)
    
    args = parser.parse_args()

    sys.exit(main(args))

import os, sys
import fnmatch
import logging
import psycopg2
from geo.Geoserver import Geoserver
from geoserver.catalog import Catalog


def asgsDB_connect():
    conn = None
    cursor = None

    user = os.getenv('DB_USER', 'user')
    pswd = os.getenv('DB_PSWD', 'password')
    host = os.getenv('DB_HOST', '172.25.16.10')
    port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'asgs_dashboard')

    try:
        conn_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, db_name, user, pswd)

        conn = psycopg2.connect(conn_str)
        conn.set_session(autocommit=True)
    except:
        e = sys.exc_info()[0]
        logging.error("FAILURE - Cannot update ASGS_DB. error {0}".format(str(e)))
    finally:
        return conn


def asgsDB_close(conn):

    if (conn.cursor):
        conn.cursor.close()
    if (conn):
        conn.close()

# Add the geoserver url for this image to the DB
def asgsDB_update(instanceId, name, url):
    logging.info('Updating DB record - instance id: {} with url: {}'.format(instanceId, url))
    conn = asgsDB_connect()

    # format of mbtiles is ex: maxele.63.0.9.mbtiles
    # final key value will be in this format image.maxele.63.0.9
    key_name = "image." + os.path.splitext(name)[0]
    key_value = url

    try:
        cursor = conn.cursor()

        sql_stmt = 'INSERT INTO "ASGS_Mon_config_item" (key, value, instance_id) VALUES(%s, %s, %s)'
        params = [key_name, key_value, instanceId]

        cursor.execute(sql_stmt, params)
    except:
        e = sys.exc_info()[0]
        logging.error("FAILURE - Cannot update ASGS_DB. error {0}".format(str(e)))
    finally:
        asgsDB_close(conn)

# given an instance id and an input dir (where to find mbtiles)
# add the mbtiles to the specified GeoServer (configured with env vars)
# then update the specified DB with the access urls (configured with env vars)

def main(args):
    logging.basicConfig(filename='stage-data-load-images.log',format='%(asctime)s : %(levelname)s : %(funcName)s : %(module)s : %(name)s : %(message)s', level=logging.WARNING)

    # process args
    if not args.instanceId:
        print(f"Need instance id on command line: --instanceId <instanceid>")
        return 1
    instance_id = args.instanceId.strip()

    user = os.getenv('GEOSERVER_USER', 'user')
    pswd = os.environ.get('GEOSERVER_PASSWORD', 'password')
    url = os.environ.get('GEOSERVER_URL', 'https://apsviz-geoserver.renci.org/geoserver/')
    worksp = os.environ.get('GEOSERVER_WORKSPACE', 'ADCIRC_2021')

    logging.info("Connecting to GeoServer at host: {0}".format(str(url)))
    # create a GeoServer connection
    geo = Geoserver(url, username=user, password=pswd)

    # create a new workspace
    if (geo.get_workspace(worksp) is None):
        geo.create_workspace(workspace=worksp)

    # /projects/ees/APSViz final dir path needs to be well defined
    #final_path = "/projects/ees/APSViz/final/" + instance_id
    final_path = "/projects/ees/APSViz/stageDIR"
    mbtiles_path = final_path + "/mbtiles"

    # temporary file set to test with
    tile_set = {
        "maxele.63.0.9.mbtiles",
        "maxwvel.63.0.9.mbtiles",
        "swan_HS_max.63.0.9.mbtiles",
        "maxele.63.10.10.mbtiles",
        "maxwvel.63.10.10.mbtiles",
        "swan_HS_max.63.10.10.mbtiles",
        "maxele.63.1.11.mbtiles",
        "maxwvel.63.11.11.mbtiles",
        "swan_HS_max.63.11.11.mbtiles",
        "maxele.63.12.12.mbtiles",
        "maxwvel.63.12.12.mbtiles",
        "swan_HS_max.63.12.12.mbtiles"
    }

    # format of mbtiles is ex: maxele.63.0.9.mbtiles
    # pull out meaningful pieces of file name
    # get all files in mbtiles dir and loop through
    # for file in fnmatch.filter(os.listdir(mbtiles_path), '*.mbtiles'):
    for file in tile_set:
        file_path = mbtiles_path + "/" + file
        layer_name = instance_id + "_" + file 
        logging.info('Adding layer: {} into workspace: {}'.format(layer_name, worksp))
        print('Adding layer: {} into workspace: {}'.format(layer_name, worksp))

        # create datastore and associate with .mbtiles file
        geo.create_coveragestore(lyr_name=layer_name, path=file_path, workspace=worksp, file_type='mbtiles')

        # update DB with url of layer for access from website NEED INSTANCE ID for this
        layer_url = '{0}/rest/workspaces/{1}/coveragestores/{2}.json'.format(url, worksp, layer_name)
        #asgsDB_update(instance_id, file, layer_url)


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument('--instanceId', default=None, help='instance id of db entry for this model run', type=str)
    
    args = parser.parse_args()

    sys.exit(main(args))

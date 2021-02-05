aimport os, sys
import fnmatch
import logging
import psycopg2
from geo.Geoserver import Geoserver
from geoserver.catalog import Catalog


# Add the geoserver url for this image to the DB
def update_db(instance_id, name, url):
    logging.info('Updating DB record - instance id: {} with url: {}'.format(instance_id, url))

    conn = None
    cursor = None

    user = os.getenv('DB_USER')
    pswd = os.getenv('DB_PSWD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    try:
        conn_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, db_name, user, pswd)
    
        conn = psycopg2.connect(conn_str)
        conn.set_session(autocommit=True)
        cursor = conn.cursor()

        sql_stmt = "this"
        params = "these"

        #cursor.execute(sql_stmt, params)
    except:
        e = sys.exc_info()[0]
        logging.error("FAILURE - Cannot update ASGS_DB. error {0}".format(str(e)))
    finally:
        if (cursor):
            cursor.close()
        if (conn):
            conn.close()

# given an instance id and an input dir (where to find mbtiles)
# add the mbtiles to the specified GeoServer (configured with env vars)
# then update the specified DB with the access urls (configured with env vars)

def main(args):
    logging.basicConfig(filename='stage-data-load-images.log',format='%(asctime)s : %(levelname)s : %(funcName)s : %(module)s : %(name)s : %(message)s', level=logging.WARNING)

    # process args
    if not args.inputDir:
        print(f"Need inputDir on command line: --inputDir $stageDir")
        return 1
    inputDir = args.inputDir.strip()

    if not args.instanceId:
        print(f"Need instance id on command line: --instanceId <instanceid>")
        return 1
    instance_id = args.instanceId.strip()

    user = os.getenv('GEOSERVER_USER')
    pswd = os.environ.get('GEOSERVER_PASSWORD')
    url = os.environ.get('GEOSERVER_URL')
    worksp = os.environ.get('GEOSERVER_WORKSPACE')
    print(worksp)

    # create a GeoServer connection
    geo = Geoserver(url, username=user, password=pswd)

    # create a new workspace
    if (geo.get_workspace(worksp) is None):
        geo.create_workspace(workspace=worksp)

    # get all files in mbtiles dir and loop through
    for file in fnmatch.filter(os.listdir(inputDir), '*.mbtiles'):
        file_path = inputDir + "/" + file
        layer_name = instance_id + "_" + file 
        logging.info('Adding layer: {} into workspace: {}'.format(layer_name, worksp))
        print('Adding layer: {} into workspace: {}'.format(layer_name, worksp))

        # create datastore and associate with .mbtiles file
        geo.create_coveragestore(lyr_name=layer_name, path=file_path, workspace=worksp, file_type='mbtiles')

        # update DB with url of layer for access from website NEED INSTANCE ID for this??
        layer_url = '{0}/rest/workspaces/{1}/coveragestores/{2}.json'.format(
                url, worksp, layer_name)
        update_db(instance_id, file, layer_url)

# set any status flags in DB??


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument('--inputDir', default=None, help='inputDir to retrieve data from', type=str)
    parser.add_argument('--instanceId', default=None, help='instance id of db entry for this model run', type=str)
    
    args = parser.parse_args()

    sys.exit(main(args))

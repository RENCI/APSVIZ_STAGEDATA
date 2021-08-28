from urllib.request import urlopen
from urllib.parse import urlparse
from common.logging import LoggingUtil
from enum import Enum
import logging
import json
import os


# handles editing of TerriaMap data catalog (apsviz.json)
# assumes skeleton catalog exists, with sections
# for latest results, recent runs (last 5) and archive (everything else)
#

class CatalogGroup(Enum):
    CURRENT = 0
    RECENT = 1
    ARCHIVE = 2

class TerriaCatalog:

    cat_save_path = "/projects/ees/APSViz"

    current_name = "ADCIRC Data - Current"
    recent_name = "ADCIRC Data - Recent"
    archive_name = "ADCIRC Data - Archive"

    test_cat = '{' \
        '"corsDomains": [' \
            '"corsproxy.com",' \
            '"programs.communications.gov.au",' \
            '"www.asris.csiro.au",' \
            '"mapsengine.google.com"' \
         '],' \
        '"homeCamera": {' \
            '"west": -96,' \
            '"south": 20,' \
            '"east": -61,' \
            '"north": 46' \
        '},' \
        '"baseMapName": "Bing Maps Roads",' \
        '"initialViewerMode": "2d",' \
        '"services": [],' \
        '"catalog": [' \
        '{' \
             '"name": "ADCIRC Data - Current",' \
             '"type": "group",' \
             '"preserveOrder": true,' \
             '"items": [' \
             ']' \
         '},' \
         '{' \
             '"name": "ADCIRC Data - Recent",' \
             '"type": "group",' \
             '"preserveOrder": true,' \
             '"items": [' \
             ']' \
         '},' \
         '{' \
             '"name": "ADCIRC Data - Archive",' \
             '"type": "group",' \
             '"items": [' \
             ']' \
         '}' \
         ']' \
    '}'

    cat_wms_item = '{' \
        '"isEnabled": true,' \
        '"isShown": true,' \
        '"isLegendVisible": false,' \
        '"name": "Name",' \
        '"description": "This data is produced by the ADCIRC model and presented through the ADCIRC Prediction System Visualizer",' \
        '"dataCustodian": "RENCI",' \
        '"layers": "layers",' \
        '"type": "wms",' \
        '"url": "https://apsviz-geoserver.renci.org/geoserver/ADCIRC_2021/wms",' \
        '"legendUrl": "legendUrl"' \
    '}'

    cat_wfs_item = '{' \
             '"name": "Name",' \
             '"description": "Example description",' \
             '"dataCustodian": "RENCI",' \
             '"typeNames": "typeNames",' \
             '"type": "wfs",' \
             '"url": "https://apsviz-geoserver.renci.org/geoserver/ADCIRC_2021/wfs/ADCIRC_2021?service=wfs&version=1.3.0&request=GetCapabilities",'  \
             '"featureInfoTemplate": "<div class=’stations’><figure><img src={{imageurl}}><figcaption>{{stationname}}</figcaption></figure></div>"' \
    '}'

    def __init__(self, cat_url, host, userid, userpw):

        # get the log level and directory from the environment
        log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
        log_path: str = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))

        # create the dir if it does not exist
        if not os.path.exists(log_path):
           os.mkdir(log_path)

        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.load-geoserver-images", level=log_level, line_format='medium',
                                          log_file_path=log_path)

        self.cat_url = cat_url
        self.host = host
        self.userid = userid
        self.userpw = userpw
        # load test json as default
        self.cat_json = json.loads(self.test_cat)

        self.logger.info(f'cat_url: {cat_url}')
        # get json from url, if exists
        if(cat_url is not None):
            # store the response of URL
            response = urlopen(cat_url)
            self.logger.info(f'read response: {response}')

            # storing the JSON response from url in data
            self.cat_json = json.loads(response.read())

    # create url for legend
    # need to get just basic layername and the adcirc var name
    # from the layers var which is formatted like this:
    # ADCIRC_2021:3548-14-nhcOfcl_maxwvel.63.0.9
    # or this, in the case of the swan var:
    # ADCIRC_2021:3041-2021062306-namforecast_swan_HS_max.63.0.9
    def create_legend_url(self, layers):

        self.logger.debug(f'layers: {layers}')
        parts1 = layers.split(':')
        parts2 = parts1[1].split('_')
        if (len(parts2) > 3):
            # this is the swan var name (more dashes in name)
            # make it look like the other var names
            parts2 = [parts2[0], f"{parts2[1]}_{parts2[2]}_{parts2[3]}"]
        basic_layer_name = parts2[0]
        adcirc_var_parts = parts2[1].split('.')
        legend_url = f"https://apsviz-geoserver.renci.org/obs_pngs/{basic_layer_name}/{adcirc_var_parts[0]}.{adcirc_var_parts[1]}.colorbar.png"

        return legend_url


    # overwrite current catalog items with latest
    # only two ever exists in this group - latest maxele and noaa obs
    def update_latest_results(self, latest_layers):

        self.logger.info(f'latest_layers: {latest_layers}')
        cat_item_list = self.cat_json['catalog'][CatalogGroup.CURRENT.value]['items']
        # find the wms and wfs items in this list - should only be one of each
        item_idx = 0
        for item in cat_item_list:
            if(item["type"] == "wms"):
                legend_url = self.create_legend_url(latest_layers["wms_layer"])
                cat_item_list[item_idx]["name"] = latest_layers["wms_title"]
                cat_item_list[item_idx]["layers"] = latest_layers["wms_layer"]
                cat_item_list[item_idx]["legendUrl"] = legend_url
            elif(item["type"] == "wfs"):
                cat_item_list[item_idx]["name"] = latest_layers["wfs_title"]
                cat_item_list[item_idx]["typeNames"] = latest_layers["wfs_layer"]
            item_idx += 1

        self.logger.info(f'cat_item_list: {cat_item_list}')
        # put this item list back in main catalog
        self.cat_json['catalog'][CatalogGroup.CURRENT.value]['items'] = cat_item_list

    # no group handling features for now
    # items is a list
    #def add_wms_group(self, name, type, items):
    #def rm_wms_group(self, name, type, items):
    #def add_wfs_group(self, name, type, items):
    #def rm_wfs_group(self, name, type, items):


    def create_wms_data_item(self,
                             name,
                             layers,
                             legend_url,
                             enabled,
                             shown,
                             legend_visible,
                             url,
                             description,
                             data_custodian
                             ):
        wms_item = {}
        wms_item = json.loads(self.cat_wms_item)
        wms_item["isEnabled"] = enabled
        wms_item["isShown"] = shown
        wms_item["isLegendVisible"] = legend_visible
        wms_item["name"] = name
        wms_item["description"] = description
        wms_item["dataCustodian"] = data_custodian
        wms_item["layers"] = layers
        wms_item["url"] = url
        wms_item["legendUrl"] = legend_url

        return wms_item

    def create_wfs_data_item(self,
                             name,
                             type_names,
                             enabled,
                             shown,
                             legend_visible,
                             url,
                             type,
                             description,
                             data_custodian,
                             feature_info_template
                             ):
        wfs_item = {}
        wfs_item = json.loads(self.cat_wfs_item)
        wfs_item["isEnabled"] = enabled
        wfs_item["isShown"] = shown
        wfs_item["isLegendVisible"] = legend_visible
        wfs_item["name"] = name
        wfs_item["description"] = description
        wfs_item["dataCustodian"] = data_custodian
        wfs_item["typeNames"] = type_names
        wfs_item["url"] = url
        wfs_item["type"] = type
        wfs_item["featureInfoTemplate"] = feature_info_template

        return wfs_item

        # TODO: can't think of a better way to do this right now -
        # but definately needs to change
        # removes last 4 entries (assumed oldest) in the items list
    def rm_oldest_recent_items(self):

        # get item list for this group
        cat_item_list = self.cat_json['catalog'][CatalogGroup.RECENT.value]['items']

        # remove last 4 items in the list
        num_items = 0
        for i, e in reversed(list(enumerate(cat_item_list))):
            num_items += 1
            del (cat_item_list[i])
            if (num_items >= 4):
                break

        # put this item list back into main catalog
        self.cat_json['catalog'][CatalogGroup.RECENT.value]['items'] = cat_item_list


    # put the newest items at the top and only show the last 5 runs
    # workspace, date, cycle, runtype, stormname, advisory, grid):
    # group is an ENUM - i.e. CatalogGroup.RECENT
    def add_wms_item(self,
                    name,
                    layers,
                    enabled=False,
                    shown=False,
                    legend_visible=False,
                    url = "https://apsviz-geoserver.renci.org/geoserver/ADCIRC_2021/wms/ADCIRC_2021?service=wfs&version=1.3.0&request=GetCapabilities",
                    type="wms",
                    description="This data is produced by the ADCIRC model and presented through the ADCIRC Prediction System Visualizer",
                    data_custodian="RENCI"):

        # create url for legend
        legend_url= self.create_legend_url(layers)
        self.logger.debug(f'legend_url: {legend_url}')

        # add this item to the CURRENT group in the catalog
        cat_item_list = self.cat_json['catalog'][CatalogGroup.RECENT.value]['items']
        wms_item = self.create_wms_data_item(name, layers, legend_url, enabled, shown, legend_visible, url, description, data_custodian)
        cat_item_list.insert(0, wms_item)

        # put this item list back into main catalog
        self.cat_json['catalog'][CatalogGroup.RECENT.value]['items'] = cat_item_list


    # put the newest items at the top and only show the last 5 runs - not possible?
    # group is an ENUM - i.e. CatalogGroup.RECENT
    def add_wfs_item(self,
                    name,
                    typeNames,
                    url = "https://apsviz-geoserver.renci.org/geoserver/ADCIRC_2021/wfs/ADCIRC_2021?service=wfs&version=1.3.0&request=GetCapabilities",
                    enabled=False,
                    shown=False,
                    legend_visible=False,
                    type="wfs",
                    description="NOAA Observations",
                    dataCustodian="RENCI",
                    featureInfoTemplate="<div class=’stations’><figure><img src={{imageurl}}><figcaption>{{stationname}}</figcaption></figure></div>"):

        cat_item_list = self.cat_json['catalog'][CatalogGroup.RECENT.value]['items']
        wfs_item = self.create_wfs_data_item(name, typeNames, enabled, shown, legend_visible, url, type, description, dataCustodian, featureInfoTemplate)
        cat_item_list.insert(0, wfs_item)

        # put this item list back into main catalog
        self.cat_json['catalog'][CatalogGroup.RECENT.value]['items'] = cat_item_list


    # update the TerriaMap data catalog with a list of wms and wfs layers
    # layergrp looks like this: {'wms': [{'layername': '', 'title': ''}], 'wfs': [{'layername': '', 'title': ''}]}
    def update(self, layergrp):
        self.logger.info(f'layergrp: {layergrp}')
        # make dict to save latest results maxele layer and noaa obs layer
        latest_layers = {"wms_title": "", "wms_layer": "", "wfs_title": "", "wfs_layer": ""}
        self.logger.info(f'latest_layers: {latest_layers}')
        # first take care of the WMS layers
        for wms_layer_dict in layergrp["wms"]:
            self.add_wms_item(wms_layer_dict["title"], wms_layer_dict["layername"])
            if ("maxele" in wms_layer_dict["layername"]):
                latest_layers["wms_title"] = wms_layer_dict["title"]
                latest_layers["wms_layer"] = wms_layer_dict["layername"]

        for wfs_layer_dict in layergrp["wfs"]:
            self.add_wfs_item(wfs_layer_dict["title"], wfs_layer_dict["layername"])
            latest_layers["wfs_title"] = wfs_layer_dict["title"]
            latest_layers["wfs_layer"] = wfs_layer_dict["layername"]

        self.update_latest_results(latest_layers)

        # now delete the oldest entries in the CURRENT group
        #self.rm_oldest_recent_items()

        # now save all of these updates to the catalog file
        self.save()


    # save the current version (in memory) to a local file
    # and then move that file to a remote host:/dir
    def save(self):

        tmp_path = "/data/tmp_cat.json"
        # save catalog file to local tmp file
        with open(tmp_path, 'w') as f:
            json.dump(self.cat_json, f, indent=4)

        url_parts = urlparse(self.cat_url)
        to_host = self.host
        to_path = self.cat_save_path + url_parts.path

        to_path = f"{self.userid}@{to_host}:{to_path}"
        self.logger.info(f'to_path: {to_path}')
        scp_cmd = f'scp -r -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no {tmp_path} {to_path}'
        os.system(scp_cmd)

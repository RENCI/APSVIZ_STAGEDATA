from urllib.request import urlopen
from urllib.parse import urlparse
import json
import Enum
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
        '"url": "https://apsviz-geoserver.renci.org/geoserver/ADCIRC_2021/wms"' \
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

    def __init__(self, cat_url, userid, userpw):

       self.cat_url = cat_url
       self.userid = userid
       self.userpw = userpw
       # load test json as default
       self.cat_json = json.loads(self.test_cat)

       # get json from url, if exists
       if(cat_url is not None):
           # store the response of URL
           response = urlopen(cat_url)

           # storing the JSON response from url in data
           self.cat_json = json.loads(response.read())


    # overwrite current catalog items with latest
    # only two ever exists in this group - latest maxele and noaa obs
    def update_latest_results(self, latest_layers):

        cat_item_list = self.cat_json['catalog'][CatalogGroup.CURRENT]['items']
        # find the wms and wfs items in this list - should only be one of each
        item_idx = 0
        for item in cat_item_list:
            if(item["type"] == "wms"):
                cat_item_list[item_idx]["name"] = latest_layers["wms_title"]
                cat_item_list[item_idx]["layers"] = latest_layers["wms_layer"]
            elif(item["type"] == "wfs"):
                cat_item_list[item_idx]["name"] = latest_layers["wfs_title"]
                cat_item_list[item_idx]["type_names"] = latest_layers["wfs_layer"]
            item_idx += 1

        # put this item list back in main catalog
        self.cat_json['catalog'][CatalogGroup.CURRENT]['items'] = cat_item_list

    # no group handling features for now
    # items is a list
    #def add_wms_group(self, name, type, items):
    #def rm_wms_group(self, name, type, items):
    #def add_wfs_group(self, name, type, items):
    #def rm_wfs_group(self, name, type, items):


    def create_wms_data_item(self,
                             name,
                             layers,
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
        wfs_item = json.loads(self.cat_wms_item)
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
        cat_item_list = self.cat_json['catalog'][CatalogGroup.RECENT]['items']

        # remove last 4 items in the list
        num_items = 0
        for i, e in reversed(list(enumerate(cat_item_list))):
            num_items += 1
            del (cat_item_list[i])
            if (num_items >= 4):
                break

        # put this item list back into main catalog
        self.cat_json['catalog'][CatalogGroup.RECENT]['items'] = cat_item_list


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

        # add this item to the CURRENT group in the catalog
        cat_item_list = self.cat_json['catalog'][CatalogGroup.CURRENT]['items']
        wms_item = self.create_wms_data_item(name, layers, enabled, shown, legend_visible, url, description, data_custodian)
        cat_item_list.insert(0, wms_item)

        # put this item list back into main catalog
        self.cat_json['catalog'][CatalogGroup.CURRENT]['items'] = cat_item_list


    # put the newest items at the top and only show the last 5 runs - not possible?
    # group is an ENUM - i.e. CatalogGroup.RECENT
    def add_wfs_item(self,
                    name,
                    typeNames,
                    url = "https://apsviz-geoserver.renci.org/geoserver/ADCIRC_2021/wfs/ADCIRC_2021?service=wfs&version=1.3.0&request=GetCapabilities",
                    enabled = "false",
                    shown = "false",
                    legend_visible = "false",
                    type="wfs",
                    description="NOAA Observations",
                    dataCustodian="RENCI",
                    featureInfoTemplate="<div class=’stations’><figure><img src={{imageurl}}><figcaption>{{stationname}}</figcaption></figure></div>"):

        cat_item_list = self.cat_json['catalog'][CatalogGroup.CURRENT]['items']
        wfs_item = self.create_wfs_data_item(name, typeNames, enabled, shown, legend_visible, url, type, description, dataCustodian, featureInfoTemplate)
        cat_item_list.insert(0, wfs_item)

        # put this item list back into main catalog
        self.cat_json['catalog'][CatalogGroup.CURRENT]['items'] = cat_item_list


    # update the TerriaMap data catalog with a list of wms and wfs layers
    # layergrp looks like this: {'wms': [{'layername': '', 'title': ''}], 'wfs': [{'layername': '', 'title': ''}]}
    def update(self, layergrp):
        # make dict to save latest results maxele layer and noaa obs layer
        latest_layers = {"wms_title": "", "wms_layer": "", "wfs_title": "", "wfs_layer": ""}
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

        self.update_latest_results(self, latest_layers)

        # now delete the oldest entries in the CURRENT group
        self.rm_oldest_recent_items()

        # now save all of these updates to the catalog file
        self.save()


    # save the current version (in memory) to a local file
    # and then move that file to a remote host:/dir
    def save(self):

        tmp_path = "/data/tmp_cat.json"
        # save catalog file to local tmp file
        with open(tmp_path, 'w') as f:
            json.dump(self.cat_json, f)

        url_parts = urlparse(self.cat_url)
        to_host = url_parts.hostname
        to_path = url_parts.path

        to_path = f"{self.userid}:{self.userpw}@{to_host}:{to_path}"
        scp_cmd = f'scp -r -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no {tmp_path} {to_path}'
        os.system(scp_cmd)
#!/usr/bin/env python

import os
import yaml
import requests
import io

import zipfile
import json

## methods from other classes
import utils
import pprint

## place to save data
SAVE_PATH = "data"

class FDAAPI(object):
    """
    FDA API class - to download drug label dataset
    """
    def __init__(self,configuration_file = "config.yaml"):
        self.config = utils.read_configuration(configuration_file)
        self.endpoint = self.config.get('API_ENDPOINT_PREFIX','')
        self.endpoint = f"{self.endpoint}?api_key={self.config.get('API_KEY','')}"
        self.metadata = {}

        ## read data
        ## API URLS
        self.api_urls = self.config.get("API_URLS","")

        list(map(self.download_data,self.api_urls))
    

        
    def read_metadata(self,json_object):
        pass

    def download_data(self, url):
        try:
            response = requests.get(url)
            with zipfile.ZipFile(io.BytesIO(response.content)) as zfile:
                for zipinfo in zfile.infolist():
                    filename = zipinfo.filename
                    with zfile.open(zipinfo) as f:
                        
                        ## save zip file
                        content = f.read()
                        
                        ## save json file
                        with open(os.path.join(SAVE_PATH,filename),'w') as sf:
                            sf.write(content.decode("utf-8"))

                        d = json.loads(content.decode('utf-8'))
                        # parse json content into individual files
                        self.parse_json_download(d)     
                        
        except IOError as ex:
            print(ex)

    def parse_json_download(self, json_content):
        """
        parse json download, extract individual results

        Args:
            json_content ([type]): [description]
        """
        metadata = json_content.get("meta","")
        results = json_content.get("results","")

        try:
            for result in results:
                # extract application id
                application_id = result.get("id")
                json_object = json.dumps(result, indent = 4) 
                json_filename = f"{application_id}.json"
                with open(os.path.join(SAVE_PATH,json_filename), "w") as outfile: 
                    outfile.write(json_object) 
        except IOError as ex:
            print("failed to parse json",ex)

api_obj = FDAAPI()

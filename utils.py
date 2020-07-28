#!/usr/bin/env python

import os
import yaml
import json


def read_configuration(config_file_path = "config.yaml"):
    """[summary]
    Read API key and other parameters from configuration file

    Args:
        config_file_path (str, optional): [description]. Defaults to "config.yaml".
    
    """
    with open(config_file_path, 'r') as file:
        configuration = yaml.load(file, Loader=yaml.FullLoader)

    print(configuration)
    
    return configuration
    

def pretty_print_json_response(response):
    """
    JSON response obj

    Args:
        response ([type]): [description]
    """
    
    text = json.dumps(response, sort_keys=True, indent = 4)
    print(text)

    return 

    
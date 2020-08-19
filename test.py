#!/usr/bin/env python3

import xml.etree.ElementTree as ET 
from lxml import etree, objectify
import os 
import json

from drug_label import DrugLabel

"""
Remove xml prefixes
"""
def remove_prefixes(xml_file):
    parser = etree.XMLParser(huge_tree = True)
    tree = etree.parse(xml_file, parser=parser)
    root = tree.getroot()
    for elem in root.getiterator():
        if not hasattr(elem.tag, 'find'): 
            continue  # (1)   
        i = elem.tag.find('}')
        if i >= 0:
            elem.tag = elem.tag[i+1:]
    objectify.deannotate(root, cleanup_namespaces=True)
    xml_str = etree.tostring(tree, xml_declaration = True)
    
    return xml_str

def get_json_dailyMed(xml_filename):
    with open(os.path.join("data/dailyMed/xml",xml_filename),'r') as x:
        xml_string = remove_prefixes(os.path.join("data/dailyMed/xml",xml_filename))
        dlab = DrugLabel(xml_string)
        response = dlab.process()
        sections = (response.get("sections",""))
        print(sections.keys())

        return response

def get_json_fda(json_filename):
    with open(os.path.join("data/fda",json_filename),'r') as j:
        response = json.loads(j.read())
        print(response['set_id'])
        return response
        
if __name__ == "__main__":
    xml_file = "ffa7787b-3e29-4622-b0c8-272d444bd42e.xml"
    print("Dailymed sections")
    dailyMed_json = get_json_dailyMed(xml_file)

    print("fda sections")
    fda_json_file = "ffbeae8a-8b63-4651-9b8e-817b3706faf5.json"
    fda_json = get_json_fda(fda_json_file)


    dailyMed_setId = dailyMed_json['setId']
    print("setid:{}, version:{}, effective date:{}, title:{}, genericName:{},dosageform:{}, active:{}".format(dailyMed_json['setId'],dailyMed_json['versionNumber'],
    dailyMed_json["effectiveTime"], dailyMed_json["title"], dailyMed_json["genericName"], dailyMed_json["dosageForm"], dailyMed_json["substanceName"] ))

    ## fda json
    print("setid:{}, version:{}, effective date:{}, title:{}, genericName:{},dosageform:{}, active:{}".format(fda_json['set_id'],fda_json['version'],
    fda_json["effective_time"], dailyMed_json["title"], dailyMed_json["genericName"], dailyMed_json["dosageForm"], dailyMed_json["substanceName"] ))

    
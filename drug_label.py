#!/usr/bin/env python
import json
import xml.etree.ElementTree as ET
from lxml import etree, objectify
import pprint
import pdb
from datetime import date
import string

# meta data along with text from section
import traceback


class DrugLabel(object):
    def __init__(self, xmlstr):
        p = etree.XMLParser(remove_blank_text=True, huge_tree=True)

        self.tree = ET.ElementTree(ET.fromstring(xmlstr,parser=p))
        self.tree_et = etree.ElementTree(etree.fromstring(xmlstr,parser=p))

        self.root = self.tree.getroot()
        self.xml_ns = {None: 'urn:hl7-org:v3'}

        self.strip_newline_tab = lambda x: x.strip("\n\t ") if x != None else ""

        ## adding regular expression name space for case insensitive matching
        self.ns = {"re": "http://exslt.org/regular-expressions"}

    def process(self):
        # extract 
        # code, systemName and displayName
        response = {}
        try:
            summary = self.extract_summary()
            sections, section_text = self.extract_text_sections
            response.update(summary)
            response["sections"] = sections
            response["sectionText"] = section_text

        except Exception as e:
            tb = traceback.format_exc()
            print(tb)
            raise Exception("error occurred value processing xml", e)

        return response

    def extract_summary(self):
        """
        method to extra meta data from the document
        - drug name, ndc code, revised ndc code,  
        - setid, document id, drug class, active moiety, active ingredient,
        - inactive ingredient, any ingredient, 
        - dosage form,
        - product type,
        - consumedIn
        - marketing date,
        - marketing category
        
        """
        metadata = {}

        ## document Id
        documentId = self.tree.find("./id")
        documentId = documentId.attrib['root'] if documentId is not None and "root" in documentId.attrib else ""
        metadata["documentId"] = documentId

        ## setId
        setid = self.tree.find("./setId")
        setid = setid.attrib['root'] if setid is not None and "root" in setid.attrib else ""
        metadata["setId"] = setid

        ## version number
        splversion = self.tree.find("./versionNumber")
        versionNumber = ""
        if splversion is not None:
            if "value" in splversion.attrib:
                versionNumber = splversion.attrib["value"]
        metadata["versionNumber"] = versionNumber

        ## product type   
        code = self.tree.find("./code")
        check_if_attrib_exists = lambda x, key: x[key] if key in x else ''
        product_type = check_if_attrib_exists(code.attrib, "displayName")
        metadata["productType"] = product_type

        ## title
        title_text = self.tree_et.xpath("./title//text()")
        title = (" ".join([self.strip_newline_tab(t) for t in title_text]) if len(title_text) > 0 else "")
        metadata["title"] = title

        ## manufacturer
        manufacturer = self.tree.find("./author//representedOrganization/name")
        if manufacturer != None and manufacturer.text != None:
            manufacturer = self.strip_newline_tab(manufacturer.text)
        else:
            manufacturer = ""
        metadata["manufacturer"] = manufacturer

        ## effectivetime
        effectiveTime = self.tree_et.xpath("./effectiveTime/@value")
        effectiveTime = self.__normalize_date(effectiveTime)

        metadata["effectiveTime"] = effectiveTime
        metadata["publishedDate"] = effectiveTime

        ## From manufacturedProduct section
        brand_name = self.tree_et.xpath(".//manufacturedProduct//name")
        brand_name = self.strip_newline_tab(brand_name[0].text) if len(brand_name) > 0 else ""
        metadata["drugName"] = brand_name

        route = self.tree_et.xpath(".//manufacturedProduct//formCode/@code")
        route = self.strip_newline_tab(route[0]) if len(route) > 0 else ""
        metadata["routeOfAdministration"] = route

        product_ndc = self.tree_et.xpath(".//manufacturedProduct//code/@code")
        product_ndc = self.strip_newline_tab(product_ndc[0]) if len(product_ndc) > 0 else ""
        metadata["ndcCode"] = product_ndc

        generic_name = self.tree_et.xpath(".//manufacturedProduct//asEntityWithGeneric//genericMedicine/name")
        generic_name = self.strip_newline_tab(generic_name[0].text) if len(generic_name) > 0 else ""
        metadata["genericName"] = generic_name

        ## dosage form
        dosage_form = self.tree_et.xpath(".//manufacturedProduct//formCode/@displayName")
        dosage_form = dosage_form[0] if len(dosage_form) > 0 else ""
        metadata["dosageForm"] = dosage_form

        # active ingredients
        substance_name = sorted([self.strip_newline_tab(a.text) for a in
                                 self.tree_et.xpath(".//.//manufacturedProduct//activeMoiety/activeMoiety/name")])
        substance_name = ", ".join(set(substance_name))
        metadata["substanceName"] = substance_name

        ## inactive ingredients
        inactive_ingredients = sorted([self.strip_newline_tab(inactive.text) for inactive in self.tree_et.xpath(
            ".//manufacturedProduct//inactiveIngredient/inactiveIngredientSubstance/name")])

        if len(inactive_ingredients) == 0:
            inactive_ingredients = ""
        else:
            inactive_ingredients = ",".join(set(inactive_ingredients))

        metadata["inactiveIngredients"] = inactive_ingredients

        ## other ingredients
        ingredients = sorted([self.strip_newline_tab(ingredient.text) for ingredient in
                              self.tree_et.xpath(".//manufacturedProduct//ingredient/ingredientSubstance/name")])

        if len(ingredients) == 0:
            ingredients = ""
        else:
            ingredients = ", ".join(set(ingredients))
        metadata["ingredients"] = ingredients

        # marketing_category
        marketing_category = self.tree_et.xpath(".//manufacturedProduct/subjectOf/approval/code/@displayName")
        marketing_category = self.strip_newline_tab(marketing_category[0]) if len(marketing_category) > 0 else ""
        metadata["marketingCategory"] = marketing_category

        # consumed in
        consumed_in = self.tree_et.xpath(
            ".//manufacturedProduct//consumedIn/substanceAdministration/routeCode/@displayName")
        consumed_in = consumed_in[0] if len(consumed_in) > 0 else ""
        metadata["consumedIn"] = consumed_in

        # revision date
        marketing_date = self.tree_et.xpath(".//manufacturedProduct//marketingAct/effectiveTime/low/@value")
        marketing_date = self.__normalize_date(marketing_date)
        metadata["marketingDate"] = marketing_date

        return metadata

    @property
    def extract_text_sections(self):

        ## get all sections
        query = "./component/structuredBody/component/section"
        sections = self.tree_et.xpath(query)
        response = {}

        section_text = ""

        for index, sec in enumerate(sections):
            sec_name =  (self.strip_newline_tab(sec.xpath(".//@displayName")[0])) if len(
                sec.xpath(".//@displayName")) > 0 and sec.xpath(".//@displayName")!= None else f"section{index}"
            
            ## check if section has components
            component_sections = self.__check_section_has_components(sec)
            has_components = len(component_sections) > 0
            content = self.__extract_section_text(sec)

            # = content
            section_text += sec_name + "\n" + content
            innertext = ""
            if has_components:
                innertext = self.__recursively_get_text(sec)
                
            key = self.__convert_text_title_case(sec_name)
            ## section repeated multiple times
            if key in response:
                title = sec.xpath("./title")
                if len(title) > 0:
                    title = title[0].text or ""
                else:
                    title = ""
                
                text = title + "\n" + content + "\n" + innertext
                section_text += text
                response[key] += text
            else:
                text = sec_name + "\n" + content + "\n" + innertext
                section_text += text
                response[key] = text


        return response, section_text

    #### Private methods - helpers
    def __get_component_section(self, section_name):
        query = "./component/structuredBody/component/section[re:test(./code/@displayName, '^{0}$', 'i')]".format(
            section_name)
        section = self.tree_et.xpath(query, namespaces=self.ns)
        return section

    def __check_section_has_components(self, section_xml):
        components = section_xml.xpath("./component/section")
        return components

    def __extract_section_text(self, section):
        content = ""
        content = section.xpath("./text//descendant-or-self::*/text()")
        return " ".join(content).encode("ascii", "ignore").decode("utf-8") if len(content) > 0 else ""

    def __normalize_date(self, datestr):
        result = ""

        try:
            if datestr != None and len(datestr) > 0:
                datestr = datestr[0]
                year, month, day = int(datestr[0:4]), int(datestr[4:6]), int(datestr[6:])
                result = date(year, month, day).strftime("%b %d, %Y")
            else:
                result = ""
        except Exception as ex:
            print(ex)
            return ""

        return result

    def __recursively_get_text(self, sec):
        response = ""
        component_sections = self.__check_section_has_components(sec)
        for compsec in component_sections:
            has_components = self.__check_section_has_components(compsec)
            title = compsec.xpath("./title")

            if len(title) > 0:
                title = title[0].text or ""
            else:
                title = ""

            response +=title
            content = self.__extract_section_text(compsec)
            if content is not None:
                response += "\n" + content + "\n"

            if has_components:
                innertext = self.__recursively_get_text(compsec)
                if innertext != None:
                    response += innertext

        return response

    def __convert_text_title_case(self, text):
        text = "".join([x.lower() if index == 0 else x.title() for index, x in enumerate(text.split(" "))])
        text = text.translate(str.maketrans('', '', string.punctuation)) if text != None else ""
        return text



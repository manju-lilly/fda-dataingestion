#!/usr/bin/env python
import os
import pandas as pd
import csv
import pdb
import numpy as np
import sqlite3

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey

## global variables
SQLITE = 'sqlite'
DATA_DIR = "data/drugs@fda_txtfiles/"

FDA_Files = {
    "ActionTypes_Lookup": "ActionTypes_Lookup.txt",
    "ApplicationDocs": "ApplicationDocs.txt",
    "Applications": "Applications.txt",
    "ApplicationsDocsType_Lookup": "ApplicationsDocsType_Lookup.txt",
    "MarketingStatus": "MarketingStatus.txt",
    "MarketingStatus_Lookup": "MarketingStatus_Lookup.txt",
    "Products": "Products.txt",
    "SubmissionClass_Lookup": "SubmissionClass_Lookup.txt",
    "SubmissionPropertyType": "SubmissionPropertyType.txt",
    "Submissions": "Submissions.txt",
    "TE": "TE.txt"
}

# Table Names
ACTIONTYPES_LOOKUP = 'action_types_lookup'
APPLICATION_DOCS = 'application_docs'
APPLICATIONS = 'applications'
APPLICATION_DOCSTYPE_LOOKUP = "application_docs_type_lookup"
MARKETINGSTATUS = "marketing_status"
MARKETINGSTATUS_LOOKUP = "marketing_status_lookup"
PRODUCTS = "products"
SUBMISSION_CLASS_LOOKUP = "submission_class_lookup"
SUBMISSION_PROPERTY_TYPE = "submission_property_type"
SUBMISSIONS = "submissions"
TE = "te"

class FDADatabase:
    # sqlalchemy
    DB_ENGINE = {
        SQLITE: 'sqlite:///{DB}'
    }

    ## db connection - ref obj
    db_engine = None
    def __init__(self, dbtype, username='', password='', dbname=''):
        dbtype = dbtype.lower()
        if dbtype in self.DB_ENGINE.keys():
            engine_url = self.DB_ENGINE[dbtype].format(DB=dbname)
            self.db_engine = create_engine(engine_url)
            print(self.db_engine)
        else:
            print("DBType is not found in DB_ENGINE")

    def create_db_table(self):
        metadata = MetaData()

        #ActionTypes_LookupID	ActionTypes_LookupDescription	SupplCategoryLevel1Code	#SupplCategoryLevel2Code
        action_type_lookup_tb = Table(ACTIONTYPES_LOOKUP, metadata,
        Column('id', Integer, primary_key=True),
        Column('description', String),
        Column('supplCategoryLevel1Code', String),
        Column('supplCategoryLevel2Code', String))
        """
        ## ApplicationDocsID	ApplicationDocsTypeID	ApplNo	SubmissionType	SubmissionNo	ApplicationDocsTitle	ApplicationDocsURL	ApplicationDocsDate
        application_docs_tb = Table(APPLICATION_DOCS, metadata, Column('id', Integer, primary_key=True),
                                    Column('docsTypeId', Integer),
                                    Column('applNo', Integer),
                                    Column('submissionType', String), Column('submissionNo', Integer),
                                    Column("applicationDocsTitle", String),
                                    Column("applicationDocsURL", String),
                                    Column("applicationDocsDate", String)
                                    )

        #ApplNo	ApplType	ApplPublicNotes	SponsorName
        # 000004	NDA		PHARMICS
        application_tb = Table(APPLICATIONS, metadata, Column('applNo', Integer, primary_key=True),
        Column('applType', String), Column("applPublicNotes", String), Column("SponsorName", String))
        
        ## ApplicationDocsType
        # ApplicationDocsType_Lookup_ID	ApplicationDocsType_Lookup_Description
        application_doc_type_tb = Table(APPLICATION_DOCSTYPE_LOOKUP, metadata, Column('id', Integer, primary_key=True), Column('description', String))
        
        # MarketingStatus
        # MarketingStatusID	ApplNo	ProductNo
        #3	000004	004
        marketing_status_tb = Table(MARKETINGSTATUS, metadata, Column('id', Integer, primary_key=True), Column('applNo', Integer), Column('productNo', Integer))
        
        # MarketingStatus lookup
        #MarketingStatusID	MarketingStatusDescription
        marketing_status_type_tb = Table(MARKETINGSTATUS_LOOKUP, metadata, Column('id', Integer, primary_key=True), Column('description', String))
        
        # ApplNo	ProductNo	Form	Strength	ReferenceDrug	DrugName	ActiveIngredient	ReferenceStandard
        # 000004	004	SOLUTION/DROPS;OPHTHALMIC	1%	0	PAREDRINE	HYDROXYAMPHETAMINE HYDROBROMIDE	0
        products_tb = Table(PRODUCTS, metadata, Column('applNo', Integer), Column('productNo', Integer), Column('form', String), Column('strength', String), Column('referenceDrug', String), Column('drugName', String),
        Column('activeIngredient', String))
        
        ##
        # SubmissionClassCodeID	SubmissionClassCode	SubmissionClassCodeDescription
        # 1	BIOEQUIV	Bioequivalence
        ##
        submission_class_lookup_tb = Table(SUBMISSION_CLASS_LOOKUP, metadata, Column('id',Integer,primary_key=True),Column('submissionClassCode',String),Column('submissionClassDescription',String))
        
        ## Submission PropertyType
        #ApplNo SubmissionType SubmissionNo SubmissionPropertyTypeCode SubmissionPropertyTypeID
        #000159	ORIG      	1	Null	0
        submission_prop_type_code_tb = Table(SUBMISSION_PROPERTY_TYPE, metadata, Column('applNo', Integer),
        Column('submissionType', String), Column("submissionNo", Integer), Column("submissionPropertyTypeCode", String),
        Column("submissionPropertyTypeId", Integer))
        
        ## ApplNo	ProductNo	MarketingStatusID	TECode
        # 003444	001	1	AA
        te_tb = Table(TE, metadata, Column('applNo',Integer),Column('productNo',Integer),Column('marketingStatusId',Integer),Column('teCode',String))
        """
        try:
            metadata.create_all(self.db_engine)
            print("Tables created")

        except Exception as e:
            print("Error occurred during Table creation!")
            print(e)

    def execute_query(self, query=''):
        if query == '':
            return
        print(query)
        with self.db_engine.connect() as connection:
            try:
                connection.execute(query)
            except Exception as e:
                print(e)

    def insert_action_type_lookup(self, data):
        conn = sqlite3.connect("data/database/fda.db")
        cur = conn.cursor()
        try:
            types = []
            ids = []
            for row in data:
                #ActionTypes_LookupID	ActionTypes_LookupDescription	
                # SupplCategoryLevel1Code	SupplCategoryLevel2Code
                id = int(row[0])
                desc = row[1] if row[1] else ""
                code1 = row[2] if row[2] else  ""
                code2 = row[3] if row[3] else ""
                if id not in ids:
                    ids.append(id)
                else:
                    print("id exists", id)
                types.append((id,desc,code1,code2))

            
            cur.executemany(
                'INSERT or IGNORE INTO action_types_lookup VALUES (?,?,?,?)', types)

        except Exception as ex:
            print(ex)
                
        conn.commit()
        cur.close()

    def insert_into_application_docs(self, data):
        conn = sqlite3.connect("data/database/fda.db")
        cur = conn.cursor()
        try:
            types = []
            ids = []
            for row in data:
                ## ApplicationDocsID	ApplicationDocsTypeID	ApplNo	SubmissionType	SubmissionNo	ApplicationDocsTitle	ApplicationDocsURL	ApplicationDocsDate

                id = int(row[0])
                docTypeId = int(row[1]) if row[1] else None
                applNo = int(row[2]) if row[2] else None
                subtype = row[3] if row[3] else ""
                subno = int(row[4]) if row[4] else ""
                appDocTitle = row[5] if row[5] else ""
                applDocUrl = row[6] if row[6] else ""
                applDate = row[7] if row[7] else ""
                
                types.append((id, docTypeId, applNo, subtype,subno,
                              appDocTitle, applDocUrl, applDate))

            cur.executemany(
                'INSERT or IGNORE INTO application_docs VALUES (?,?,?,?,?,?,?,?)', types)

        except Exception as ex:
            print(ex)

        conn.commit()
        cur.close()
        
    def insert_into_application(self, data):
        conn = sqlite3.connect("data/database/fda.db")
        cur = conn.cursor()
        #ApplNo	ApplType	ApplPublicNotes	SponsorName

        try:
            types = []
            ids = []
            for row in data:
                #ApplNo	ApplType	ApplPublicNotes	SponsorName

                applNo = int(row[0]) if row[0] else None
                appltype = row[1] if row[1] else ""
                applPublicNotes = row[2] if row[2] else ""
                sponsorName = row[3] if row[3] else ""

                types.append((applNo, appltype, applPublicNotes,
                              sponsorName))

            cur.executemany(
                'INSERT or IGNORE INTO applications VALUES (?,?,?,?)', types)

        except Exception as ex:
            print(ex)

        conn.commit()
        cur.close()

    def insert_into_application_docstype(self, data):
        conn = sqlite3.connect("data/database/fda.db")
        cur = conn.cursor()
        #ApplNo	ApplType	ApplPublicNotes	SponsorName

        try:
            types = []
            ids = []
            for row in data:
                #ApplicationDocsType_Lookup_ID	ApplicationDocsType_Lookup_Description
                id = int(row[0]) if row[0] else None
                desc = row[1] if row[1] else ""
                
                types.append((id,desc))

            cur.executemany(
                'INSERT or IGNORE INTO application_docs_type_lookup VALUES (?,?)', types)

        except Exception as ex:
            print(ex)

        conn.commit()
        cur.close()

    def insert_into_marketing_status(self, data):
        conn = sqlite3.connect("data/database/fda.db")
        cur = conn.cursor()

        #MarketingStatusID	ApplNo	ProductNo
        try:
            types = []
            ids = []
            for row in data:
                id = int(row[0]) if row[0] else None
                applNo = int(row[1]) if row[1] else None
                productNo = int(row[2]) if row[2] else None

                types.append((id, applNo, productNo))

            cur.executemany(
                'INSERT or IGNORE INTO marketing_status VALUES (?,?,?)', types)

        except Exception as ex:
            print(ex)

        conn.commit()
        cur.close()

    def insert_into_marketingstatus_lookup(self, data):
        conn = sqlite3.connect("data/database/fda.db")
        cur = conn.cursor()

        #MarketingStatusID	ApplNo	ProductNo
        try:
            types = []
            ids = []
            for row in data:
                id = int(row[0]) if row[0] else None
                desc = row[1] if row[1] else None
 
                types.append((id, desc))

            cur.executemany(
                'INSERT or IGNORE INTO marketing_status_lookup VALUES (?,?)', types)

        except Exception as ex:
            print(ex)

        conn.commit()
        cur.close()
        
    def insert_into_products(self, data):
        conn = sqlite3.connect("data/database/fda.db")
        cur = conn.cursor()
        # ApplNo	ProductNo	Form	Strength	ReferenceDrug	DrugName	ActiveIngredient	ReferenceStandard

        try:
            types = []
            ids = []
            for row in data:
                applNo = int(row[0]) if row[0] else None
                productNo = int(row[1]) if row[1] else None
                form = row[2] if row[2] else None
                strength = row[3] if row[3] else None
                refdrug = row[4] if row[4] else None
                drugName = row[5] if row[5] else None
                activeIngredient = row[6] if row[6] else None
                refstandard = row[7] if row[7] else None

                types.append((applNo, productNo, form, strength, refdrug,
                              drugName, activeIngredient, refstandard))

            cur.executemany(
                'INSERT or IGNORE INTO products VALUES (?,?,?,?,?,?,?,?)', types)

        except Exception as ex:
            print(ex)

        conn.commit()
        cur.close()

    def insert_into_submission_class_lookup(self, data):
        conn = sqlite3.connect("data/database/fda.db")
        cur = conn.cursor()
        # id, code, desc
        try:
            types = []
            ids = []
            for row in data:
                id = int(row[0]) if row[0] else None
                code = (row[1]) if row[1] else None
                desc = row[2] if row[2] else None

                types.append((id, code, desc))

            cur.executemany(
                'INSERT or IGNORE INTO submission_class_lookup VALUES (?,?,?)', types)

        except Exception as ex:
            print(ex)

        conn.commit()
        cur.close()

    def insert_into_submissions(self, data):
        conn = sqlite3.connect("data/database/fda.db")
        cur = conn.cursor()
        # ApplNo	SubmissionClassCodeID	SubmissionType	SubmissionNo	SubmissionStatus	SubmissionStatusDate	SubmissionsPublicNotes	ReviewPriority
        try:
            types = []
            ids = []
            for row in data:
                applNo = int(row[0]) if row[0] else None
                subclasscodeId = int(row[1]) if row[1] else None
                subType = row[2] if row[2] else None
                subNo = int(row[3]) if row[3] else None
                subStatus = row[4] if row[4] else None
                subDate = row[5] if row[5] else None
                subPublicNotes = row[6] if row[6] else None
                reviewPriority = row[7] if row[7] else None

                types.append(
                    (applNo, subclasscodeId, subType, subNo, subStatus, subDate, subPublicNotes, reviewPriority))

            cur.executemany(
                'INSERT or IGNORE INTO submissions VALUES (?,?,?,?,?,?,?,?)', types)

        except Exception as ex:
            print(ex)

        conn.commit()
        cur.close()


    def insert_into_submission_property_type(self, data):
        conn = sqlite3.connect("data/database/fda.db")
        cur = conn.cursor()
        # ApplNo	SubmissionType	SubmissionNo	SubmissionPropertyTypeCode	SubmissionPropertyTypeID

        try:
            types = []
            ids = []
            for row in data:
                applNo = int(row[0]) if row[0] else None
                submissionType = (row[1]) if row[1] else None
                submissionNo = int(row[2]) if row[2] else None
                submissionTypeCode = row[3] if row[3] else None
                submissionPropertyTypeID = int(row[4]) if row[4] else None
                
                types.append((applNo, submissionType, submissionNo, submissionTypeCode,
                              submissionPropertyTypeID))

            cur.executemany(
                'INSERT or IGNORE INTO submission_property_type VALUES (?,?,?,?,?)', types)

        except Exception as ex:
            print(ex)

        conn.commit()
        cur.close()

    
    
        conn = sqlite3.connect("data/database/fda.db")
        cur = conn.cursor()
        # ApplNo	productNo, marketingStatusId, teCode
        try:
            types = []
            ids = []
            for row in data:
                applNo = int(row[0]) if row[0] else None
                productNo =int(row[1]) if row[1] else None
                marketingStatusId = int(row[2]) if row[2] else None
                teCode = (row[3]) if row[3] else None

                types.append((applNo, productNo, marketingStatusId, teCode))

            cur.executemany(
                'INSERT or IGNORE INTO te VALUES (?,?,?,?)', types)

        except Exception as ex:
            print(ex)

        conn.commit()
        cur.close()
        
def read_data(fname):
    rows = []
    header = []
    file = open(fname, 'r')

    def split_line_into_words(line): return [w for w in line.split('\t')]
    
    for index, line in enumerate(file.readlines()):
        if index == 0:
            header = split_line_into_words(line)
            continue
        data = split_line_into_words(line)
        rows.append(data)
    return (header,rows)

if __name__ == "__main__":
    # read text files
    
    # action types
    (action_type_colnames, action_types_data) = read_data(os.path.join(DATA_DIR, FDA_Files.get("ActionTypes_Lookup", "")))
    
    # application docs
    (application_docs_colnames, application_docs_data) = read_data(
        os.path.join(DATA_DIR, FDA_Files.get("ApplicationDocs", "")))
    
    # applications
    (applications_colnames, applications_data) = read_data(
        os.path.join(DATA_DIR, FDA_Files.get("Applications", "")))

    # application doc type
    (application_doc_type_colnames, applications_doc_type_data) = read_data(os.path.join(DATA_DIR, FDA_Files.get("ApplicationsDocsType_Lookup")))

    # marketing status
    (marketing_status_colnames, marketing_status_data) = read_data(os.path.join(DATA_DIR, FDA_Files.get("MarketingStatus", "")))
    
    # marketing status lookup
    (marketing_status_lookup_colnames, marketing_status_lookup_data) = read_data(os.path.join(DATA_DIR, FDA_Files.get("MarketingStatus_Lookup", "")))
    
    # products
    (products_colnames, products_data) = read_data(os.path.join(DATA_DIR, FDA_Files.get("Products", "")))
    
    # submission class lookup
    (submission_class_lookup_colnames, submission_class_lookup_data) = read_data(
        os.path.join(DATA_DIR, FDA_Files.get("SubmissionClass_Lookup", "")))

    # submission property type
    (submission_prop_type_colnames, submission_prop_type_data) = read_data(os.path.join(DATA_DIR, FDA_Files.get("SubmissionPropertyType", "")))
    
    # submissions
    (submissions_colnames, submissions_data) = read_data(os.path.join(DATA_DIR, FDA_Files.get("Submissions", "")))
    
    # TE
    (te_colnames, te_data) = read_data(os.path.join(DATA_DIR,FDA_Files.get("TE","")))


    ## initiate DB class
    fdaDB = FDADatabase(dbtype='sqlite', dbname='fda')
    
    #fdaDB.insert_action_type_lookup(action_types_data)
    #fdaDB.insert_into_application_docs(application_docs_data)
    #fdaDB.insert_into_application(applications_data)
    #fdaDB.insert_into_application_docstype(applications_doc_type_data)

    # marketing status 
    #fdaDB.insert_into_marketing_status(marketing_status_data)
    #fdaDB.insert_into_marketingstatus_lookup(marketing_status_lookup_data)
    #fdaDB.insert_into_products(products_data)
    #fdaDB.insert_into_submission_class_lookup(submission_class_lookup_data)
    #fdaDB.insert_into_submission_property_type(submission_prop_type_data)
    #fdaDB.insert_into_te(te_data)
    #fdaDB.insert_into_submissions(submissions_data)




#-------------------------------------------------------------------------------
# Name:        Metadata Generator Collector
# Purpose:     Extract meta data from databases for future data dictionary app.
#
# Author:      John Spence
#
# Created:     17 February 2022
# Modified:
# Modification Purpose:
#
#
#-------------------------------------------------------------------------------


# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
#   To be completed.
#
# ------------------------------- Dependencies ---------------------------------
# 1) 
# 2) 
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Data Sources GIS Databases
sourcesGIS_DB = [(r'D:\TestDrive\Main.sde'), (r'D:\TestDrive\Web.sde'), 
             (r'D:\TestDrive\Test.sde')]

# Data Sources MS SQL Databases
sourcesMSSQL_DB = [(r'GISData\Test', 'Carta')]

# Data Sources Oracle Databases
sourcesOracle = []

# Configure hard coded db connection here.
db_conn = ('Driver={ODBC Driver 17 for SQL Server};'  # This will require adjustment if you are using a different database.
                      r'Server=GISPRODDB\GIS;'
                      'Database=GISDBA;'
                      'Trusted_Connection=yes;'  #Only if you are using a AD account.
                      #r'UID=;'  # Comment out if you are using AD authentication.
                      #r'PWD='     # Comment out if you are using AD authentication.
                      )

# Send confirmation of rebuild to
adminNotify = 'gisdba@gis.dev'

# Configure the e-mail server and other info here.
mail_server = '	smtp-relay.gmail.com'
mail_from = 'Metadata Capture<noreply@gis.dev>'

# MS Teams Message Hook
teamsMSGHook = ''

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

# Import Python libraries
import os
import datetime
import time
import smtplib
import string
import re
import pyodbc
import json
import collections
import urllib
import requests
import concurrent.futures
import pymsteams

# For GIS Data Only
if len(sourcesGIS_DB) > 0:
    import arcpy
    import inspect
    from arcpy import metadata as md
    import xml.etree.ElementTree as ET
    from bs4 import BeautifulSoup

#-------------------------------------------------------------------------------
#
#
#                                 Functions
#
#
#-------------------------------------------------------------------------------

def main():
#-------------------------------------------------------------------------------
# Name:        Function - main
# Purpose:  Starts the whole thing.
#-------------------------------------------------------------------------------

    checkWorkspace()
    captureGISDB()
    captureMSSQLDB()

    return ()

def getqueryDBConn(dbInstance, dbName):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    # Configure hard coded db connection here.
    db_connQuery = ('Driver={ODBC Driver 17 for SQL Server};'  # This will require adjustment if you are using a different database.
                          r'Server=%s;'
                          'Database=%s;'
                          'Trusted_Connection=yes;'  #Only if you are using a AD account.
                          #r'UID=;'  # Comment out if you are using AD authentication.
                          #r'PWD='     # Comment out if you are using AD authentication.
                          % (dbInstance, dbName))

    return (db_connQuery)

def checkWorkspace():
#-------------------------------------------------------------------------------
# Name:        Function - checkWorkspace
# Purpose:  
#-------------------------------------------------------------------------------

    conn = pyodbc.connect(db_conn)
    cursor = conn.cursor()

    sqlCommand = '''
    IF OBJECT_ID ('[DBO].[META_DataSets]' , N'U') IS NULL
		    Begin
                CREATE TABLE [DBO].[META_DataSets](
                    [DBName] [NVARCHAR] (50) NULL
                    , [Schema] [NVARCHAR] (50) NULL
                    , [Table] [NVARCHAR] (255) NULL
                    , [GISData] [NVARCHAR] (3) NULL
                    , [IsView] [NVARCHAR] (3)  NULL
                    , [Description] [NVARCHAR] (2000) NULL
                    , [CountRecords] [NUMERIC] (12, 0) NULL
                    , [CountFields] [NUMERIC] (12, 0) NULL
                    , [LastRecordUpdate] [DATETIME2] (7) NULL
                    , [TimeZoneUsed] [NVARCHAR] (3) NULL
                    , [SysCaptureDate] [DATETIME2] (7) NULL
                    , [GlobalID] [UNIQUEIDENTIFIER] NOT NULL
                )
            End
    '''
    cursor.execute(sqlCommand)
    conn.commit()

    sqlCommand = '''
    IF OBJECT_ID ('[DBO].[META_DataSetsFields]' , N'U') IS NULL
		    Begin
                CREATE TABLE [DBO].[META_DataSetsFields](
                    [FieldName] [NVARCHAR] (128) NULL
                    , [FieldAlias] [NVARCHAR] (255) NULL
                    , [FieldLength] [INT] NULL
                    , [FieldType] [NVARCHAR] (100) NULL
                    , [SysCaptureDate] [DATETIME2] (7) NULL
                    , [FkID] [UNIQUEIDENTIFIER] NOT NULL
                    , [GlobalID] [UNIQUEIDENTIFIER] NOT NULL
                )
            End
    '''
    cursor.execute(sqlCommand)
    conn.commit()

    sqlCommand = '''
    IF OBJECT_ID ('[DBO].[META_altTableDesc]' , N'U') IS NULL
		    Begin
                CREATE TABLE [DBO].[META_altTableDesc](
                    [DBName] [NVARCHAR] (50) NULL
                    , [Schema] [NVARCHAR] (50) NULL
                    , [Table] [NVARCHAR] (255) NULL
                    , [Description] [NVARCHAR] (2000) NULL
                    , [RecordUpdateField] [DATETIME2] (7) NULL
                    , [TimeZoneSetting] [NVARCHAR] (3) NULL
                    , [SysCaptureDate] [DATETIME2] (7) NULL
                    , [GlobalID] [UNIQUEIDENTIFIER] NOT NULL
                )
            End
    '''
    cursor.execute(sqlCommand)
    conn.commit()
    conn.close()

    return ()

def captureMSSQLDB():
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------
    
    GISData = False
    for dbcheck in sourcesMSSQL_DB:
        itemCount = 0
        recordsCount = 0 
        dbInstance = dbcheck[0]
        dbName = dbcheck[1]

        prepWorkSpace(dbName)
        tableData = getMSSQLWorkList(dbInstance, dbName)

        for inTable in tableData:
            schemaName = inTable[1]
            tableName = inTable[2]
            print (dbName + '.' + schemaName + '.' + tableName)
            if inTable[3] == 'BASE TABLE':
                isView = False
            else:
                isView = True

            db_return = getaltTableData(dbInstance, dbName, schemaName, tableName)

            if db_return != None:
                for dataReturn in db_return:
                    dataDescription = dataReturn[3]
                    altUpdateField = dataReturn[4]
                    altTimeZone = dataReturn[5]
            else:
                dataDescription = None
                altUpdateField = None
                altTimeZone = None

            print ('    Description: {}'.format(dataDescription))

            if isView == True:
                print ('    View: Yes')
            else:
                print ('    View: No')

            db_returnFields = getMSSQLFields(dbInstance, dbName, schemaName, tableName)

            if db_returnFields != None:
                dbfieldsCount = len(db_returnFields)
                print ('    Number of Fields: {}'.format(dbfieldsCount))
            else:
                print ('    Number of Fields: Not available')
                dbfieldsCount = 0

            try:
                pendCount = getMSSQLReCount(dbInstance, dbName, schemaName, tableName)
                pendCount = int(pendCount[0])
            except:
                pendCount = 0

            print ('    Total records: {}'.format(pendCount))

            if altUpdateField != None:
                for dateField in db_returnFields:
                    if lookUp == dateField[0]:
                        updateData = captureLastModified(dbInstance, dbName, schemaName, tableName, lookUp)
                        print ('    Last Updated: {}'.format(str(updateData)))
                        utc_stat = altTimeZone

                    else:
                        updateData = altCaptureLastModifiedMSSQL(dbInstance, dbName, schemaName, tableName)
                        updateData = updateData[2]

                        print ('    Last Updated!: {}'.format(updateData))
                        utc_stat = 'PST'
                    print ('    Time Zone: {}'.format(utc_stat))
            else:
                try:
                    updateData = altCaptureLastModifiedMSSQL(dbInstance, dbName, schemaName, tableName)
                    updateData = updateData[2]
                    utc_stat = 'PST'
                    print ('    Last Updated: {}'.format(updateData))
                    print ('    Time Zone: {}'.format(utc_stat))
                except:
                    utc_stat = None
                    print ('    Last Updated: Not able to calculate')
                    print ('    Time Zone: {}'.format(utc_stat))

            sendToDBStore(dbName, schemaName, tableName, GISData, isView, dataDescription, 
                              pendCount, dbfieldsCount, updateData, utc_stat)

            print ('    Captured General Info....')

            if dbfieldsCount != 0:
                print ('    -- Transitioning to capturing field data.')
                fieldsList = db_returnFields
                sendMSSQLfieldsToDBStore(dbName, schemaName, tableName, fieldsList)
                print ('    !! Table and Fields Capture Complete for {}.{}.{}\n'.format(dbName, schemaName, tableName))
            else:
                print ('    !! Table Data Capture Complete for {}.{}.{}\n'.format(dbName, schemaName, tableName))


    return

def getMSSQLWorkList(dbInstance, dbName):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    tblTotal = 0
    tblRecordsTotal = 0
    GISData = False

    db_connQuery = getqueryDBConn(dbInstance, dbName)

    query_string = '''

    select * from INFORMATION_SCHEMA.TABLES

    '''
    query_conn = pyodbc.connect(db_connQuery)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()

    return (db_return)


def getaltTableData(dbInstance, dbName, schemaName, tableName):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    query_string = '''

    select * from [DBO].[META_altTableDesc] 
        where [DBName] = '{}' and [Schema] = '{}' and [Table] = '{}'

    '''.format(dbName, schemaName, tableName)

    query_conn = pyodbc.connect(db_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchone()
    query_cursor.close()
    query_conn.close()

    return (db_return)

def getMSSQLFields(dbInstance, dbName, schemaName, tableName):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    tblTotal = 0
    tblRecordsTotal = 0
    GISData = False

    db_connQuery = getqueryDBConn(dbInstance, dbName)

    query_string = '''

    select COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS
        where [TABLE_SCHEMA] = '{}' and [TABLE_NAME] = '{}'

    '''.format(schemaName, tableName)
    query_conn = pyodbc.connect(db_connQuery)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()

    return (db_return)

def altCaptureLastModifiedMSSQL(dbInstance, dbName, schemaName, tableName):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    db_connQuery = getqueryDBConn(dbInstance, dbName)

    query_string = '''

    SELECT distinct OBJECT_NAME(OBJECT_ID) AS TableName, ssu.name as [SchemaName],
    last_user_update,*
    FROM sys.dm_db_index_usage_stats SDDIUS 
       INNER JOIN [sys].[sysobjects] SSO
           ON SDDIUS.[object_id] = SSO.id 
       INNER JOIN sys.[sysusers] SSU  
           ON SSO.[uid] = SSU.[uid] 
    WHERE database_id = DB_ID( '{}') and ssu.name = '{}'
    AND OBJECT_ID=OBJECT_ID('{}') 
    and last_user_update is NOT NULL 

    '''.format(dbName, schemaName, tableName)

    query_conn = pyodbc.connect(db_connQuery)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchone()
    query_cursor.close()
    query_conn.close()


    return (db_return)

def getMSSQLReCount(dbInstance, dbName, schemaName, tableName):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    db_connQuery = getqueryDBConn(dbInstance, dbName)

    query_string = '''

    select count(*) from [{}].[{}].[{}]

    '''.format(dbName, schemaName, tableName)

    query_conn = pyodbc.connect(db_connQuery)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchone()
    query_cursor.close()
    query_conn.close()

    return (db_return)


def sendMSSQLfieldsToDBStore(dbName, schemaName, tableName, fieldsList):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    query_string = '''

    select [GlobalID] from [dbo].[META_DataSets] where
        [DBName] = '{}' and
        [Schema] = '{}' and
        [Table] = '{}'

    '''.format(dbName, schemaName, tableName)

    query_conn = pyodbc.connect(db_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchone()
    query_cursor.close()
    query_conn.close()
        
    tableFK = db_return[0]
    
    for fieldFC in fieldsList:
        fname = '{}'.format(fieldFC[0])
        falias = 'NULL'
        flength = fieldFC[1]
        ftype = '{}'.format(fieldFC[2])

        conn = pyodbc.connect(db_conn)
        cursor = conn.cursor()

        sqlCommand = '''

        insert into [dbo].[META_DataSetsFields] (
        [FieldName]
        , [FieldAlias]
        , [FieldLength]
        , [FieldType]
        , [SysCaptureDate]
        , [FkID]
        , [GlobalID]
        )
            Values ('{}', {}, {}, '{}', getdate(), '{}', newid())

        '''.format(fname, falias, flength, ftype, tableFK)

        #print (sqlCommand)

        cursor.execute(sqlCommand)
        conn.commit()
        conn.close()



    return



def captureGISDB():
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    fcTotal = 0
    fcRecordsTotal = 0
    GISData = True

    for dbcheck in sourcesGIS_DB:
        itemCount = 0
        recordsCount = 0
        arcpy.env.workspace = dbcheck
        descDBConn = arcpy.Describe(dbcheck)
        connProperties = descDBConn.connectionProperties
        connInstance = connProperties.instance
        connInstanceSplit = connInstance.split(':', 2)[2]
        dbInstance = r'{}'.format(connInstanceSplit)
        dbName = connProperties.database
        
        datasets = arcpy.ListDatasets(feature_type='feature')
        datasets = [''] + datasets if datasets is not None else []

        prepWorkSpace(dbName)

        for items in datasets:
            for item in arcpy.ListFeatureClasses(feature_dataset=items):
                path = os.path.join(items, item)

                if '\\' in path and 'COBNT' not in path:
                    path = path.split('\\',1)[1]

                itemData = [x for x in map(str.strip, path.split('.')) if x]
                print (dbName + '.' + itemData[1] + '.' + itemData[2])
                schemaName = itemData[1]
                tableName = itemData[2]
                #Reset the path to the original....
                path = os.path.join(items, item)

                dataDescription = getGISMetadata(path)

                print ('    Description: {}'.format(dataDescription))

                typeDescription = getEntityTypeSQLServer(dbInstance, dbName, tableName)

                if typeDescription == 'VIEW':
                    print ('    View: Yes')
                    isView = True
                elif typeDescription == 'USER_TABLE':
                    print ('    View: No')
                    isView = False
                else:
                    print ('    Type: {}'.format(typeDescription))
                    isView = False
                    
                dbfieldsCount, GISData, fieldsList = captureFieldsInfo(path)

                if dbfieldsCount != None:                
                    print ('    Number of Fields: {}'.format(dbfieldsCount))
                else:
                    print ('    Number of Fields: Not available')
                    dbfieldsCount = 0

                creatordate, editdate, utc_stat = captureEditorTrackingInfo(path)

                if utc_stat != None:
                    
                    print ('    Time Zone: {}'.format(utc_stat))
                    ET_enabled = True

                else:
                    print ('    Time Zone: No editor tracking available.')
                    ET_enabled = False

                try:           
                    recordCount = arcpy.GetCount_management(path)
                    pendCount = int(recordCount[0])
                except:
                    pendCount = 0
                    if isView == True:
                        try:
                            pendCount = altRecordCount(dbInstance, dbName, schemaName, tableName)
                        except:
                            print ('    !!! View is Broken !!!')
                print ('    Total records: {}'.format(pendCount))

                if '"' in schemaName:
                    schemaName = [x for x in map(str.strip, schemaName.split('"')) if x]
                    schemaName = r'[{}]'.format(schemaName[0])
                    schemaName = schemaName.replace ("'", '')

                if (creatordate != None or editdate != None) and ET_enabled == True and pendCount > 0:
                    if editdate != None:
                        lookUp = editdate
                    else:
                        lookUp = creatordate

                    try:
                        updateData = captureLastModified(dbInstance, dbName, schemaName, tableName, lookUp)
                    except:
                        updateData = None
                        utc_stat = None

                    if updateData == None:
                        try:
                            updateData = altLastModified(dbInstance, dbName, schemaName, tableName, lookUp)
                            utc_stat = 'PST'
                        except:
                            updateData = None
                            utc_stat = None

                    print ('    Last Updated: {}'.format(str(updateData)))
                    
                elif pendCount == 0 and ET_enabled == True:
                    try:
                        updateData = altLastModified(dbInstance, dbName, schemaName, tableName, lookUp)
                        utc_stat = 'PST'
                        print ('    Last Updated: {}'.format(str(updateData)))
                    except:
                        print ('    Last Updated: No records are present')
                else:
                    try:
                        updateData = altLastModified(dbInstance, dbName, schemaName, tableName, lookUp)
                        if ET_enabled == True:
                            utc_stat = 'ZZZ'
                        else:
                            utc_stat = 'PST'
                        print ('    Last Updated: {}'.format(str(updateData)))
                    except:
                        updateData = None
                        utc_stat = None

                        if isView == True:
                            updateData = datetime.datetime.now()
                            utc_stat = 'PST'
                            print ('    Last Updated: {}'.format(str(updateData)))
                        else:
                            print ('    Last Updated: Not able to calculate')

                sendToDBStore(dbName, schemaName, tableName, GISData, isView, dataDescription, 
                              pendCount, dbfieldsCount, updateData, utc_stat)

                print ('    Captured General Info....')

                if dbfieldsCount != 0:
                    print ('    -- Transitioning to capturing field data.')
                    sendfieldsToDBStore(dbName, schemaName, tableName, fieldsList)
                    print ('    !! Table and Fields Capture Complete for {}.{}.{}\n'.format(dbName, schemaName, tableName))
                else:
                    print ('    !! Table Data Capture Complete for {}.{}.{}\n'.format(dbName, schemaName, tableName))
        
                    
    return ()

def prepWorkSpace(dbName):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    conn = pyodbc.connect(db_conn)
    cursor = conn.cursor()

    sqlCommand = '''

    select [GlobalID] FROM [DBO].[META_DataSets] where [DBName] = '{}'

    '''.format(dbName)

    cursor.execute(sqlCommand)
    existingFields = cursor.fetchall()

    for fk in existingFields:
        sqlCommand = '''

        DELETE FROM [DBO].[META_DataSetsFields] where [FkID] = '{}'

        '''.format(fk[0])

        cursor.execute(sqlCommand)
        conn.commit()

    sqlCommand = '''

    DELETE FROM [DBO].[META_DataSets] where [DBName] = '{}'

    '''.format(dbName)

    cursor.execute(sqlCommand)
    conn.commit()

    conn.close()

    return


def getEntityTypeSQLServer(dbInstance, dbName, tableName):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    db_connQuery = getqueryDBConn(dbInstance, dbName)

    query_string = '''

    select schema_name(schema_id) as [schema], name, type, type_desc from sys.objects where name = '{}'

    '''.format(tableName)
    query_conn = pyodbc.connect(db_connQuery)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchone()
    query_cursor.close()
    query_conn.close()

    typeDescription = db_return[3]


    return(typeDescription)

def getGISMetadata(path):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    item_md = md.Metadata(path)

    #print ('{}'.format(dir(item_md)))

    if item_md.summary == None:
        summaryItem = ''
    else:
        summaryItem = item_md.summary

    if item_md.description == None:
        summaryDescription = ''
    else:
        soup = BeautifulSoup (item_md.description, 'html.parser')
        for data in soup (['style', 'script']):
            data.decompose()
        summaryDescription = (' '.join(soup.stripped_strings))

    if summaryItem != '' and summaryDescription != '':
        dataDescription = summaryItem + ' ' + summaryDescription
    elif summaryItem != '' and summaryDescription == '':
        dataDescription = summaryItem
    elif summaryItem == '' and summaryDescription != '':
        dataDescription = summaryDescription
    else:
        dataDescription = 'No data on file.'

    creditsItem = item_md.credits
    uselimfacItem = item_md.accessConstraints
    tagsItem = item_md.tags

    print ('    Credits: {}'.format(creditsItem))
    print ('    Use Limitations: {}'.format(uselimfacItem))
    print ('    Tags: {}'.format(tagsItem))


    return (dataDescription)

def captureFieldsInfo(path):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    try:
        dbfields = arcpy.ListFields (path, '*', 'ALL')
        dbfieldsCount = len(dbfields)

    except:
        dbfieldsCount = None
        GISData = False

    fieldsList = []
    
    if dbfieldsCount != None and dbfieldsCount > 0:
        fieldCheck = []
        for field in dbfields:
            #print ('    ', field.name, field.aliasName, field.type, field.length)
            fname = '{}'.format(field.name)
            falias = '{}'.format(field.aliasName)
            flength = int(field.length)
            ftype = '{}'.format(field.type)
            fcFields = (fname, falias, flength, ftype)
            fieldsList.append(fcFields)
            fieldCheck.append (field.name)
        if 'Shape' in fieldCheck:
            GISData = True
        else:
            GISData = False

    return(dbfieldsCount, GISData, fieldsList)

def captureEditorTrackingInfo(path):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    creatordate = None
    editdate = None
    try:
        desc = arcpy.Describe(path)
        if desc.editorTrackingEnabled:
            ET_enabled = True
            creator = desc.creatorFieldName
            creatordate = desc.createdAtFieldName
            editor = desc.editorFieldName
            editdate = desc.editedAtFieldName
            utc_stat = desc.isTimeInUTC
            if utc_stat == True:
                utc_stat = 'UTC'
            else:
                utc_stat = 'PST'

            #print ('    Creator Field: {}'.format(creator))
            #print ('    Create Date Field: {}'.format(creatordate))
            #print ('    Editor Field: {}'.format(editor))
            #print ('    Edit Date Field: {}'.format(editdate))
            #print ('    Time Zone: {}'.format(utc_stat))

        else:
            creatordate = None
            editdate = None
            utc_stat = None

    except:
        utc_stat = None

    return (creatordate, editdate, utc_stat)

def sendToDBStore(dbName, schemaName, tableName, GISData, isView, dataDescription, 
                  pendCount, dbfieldsCount, updateData, utc_stat):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    if GISData == True:
        GISData = "'Yes'"
    else:
        GISData = "'No'"

    if isView == True:
        isView = "'Yes'"
    else:
        isView = "'No'"

    try: 
        if len(dataDescription) == 0:
            dataDescription = 'NULL'
        elif dataDescription == None:
            dataDescription = 'NULL'
        else:
            dataDescription = dataDescription.replace('\'', '')
            dataDescription = r"'{}'".format(dataDescription)
    except:
        if dataDescription == None:
            dataDescription = 'NULL'
        else:
            dataDescription = dataDescription.replace('\'', '')
            dataDescription = r"'{}'".format(dataDescription)

    if utc_stat == None:
        updateData = 'NULL'
        utc_stat = 'NULL'
    elif updateData == None and utc_stat != None:
        updateData = 'NULL'
        utc_stat = "'{}'".format(utc_stat)
    else:
        utc_stat = "'{}'".format(utc_stat)
        updateData = "'{}'".format(updateData)

    dbName = "'{}'".format(dbName)
    schemaName = "'{}'".format(schemaName)
    tableName = "'{}'".format(tableName)


    conn = pyodbc.connect(db_conn)
    cursor = conn.cursor()

    sqlCommand = '''

    insert into [dbo].[META_DataSets] ([DBName]
      ,[Schema]
      ,[Table]
      ,[GISData]
      ,[IsView]
      ,[Description]
      ,[CountRecords]
      ,[CountFields]
      ,[LastRecordUpdate]
      ,[TimeZoneUsed]
      ,[SysCaptureDate]
      ,[GlobalID])
      Values ({}, {}, {}, {}, {}, LEFT({}, 2000), {}, {}, {}, {}, getdate(), newid())

    '''.format(dbName, schemaName, tableName, GISData, isView, dataDescription, pendCount, dbfieldsCount, updateData, utc_stat)

    cursor.execute(sqlCommand)
    conn.commit()


    return ()


def captureLastModified(dbInstance, dbName, schemaName, tableName, lookUp):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    db_connQuery = getqueryDBConn(dbInstance, dbName)

    query_string = '''

    select top 1 {} from {}.{}.{} order by {} desc

    '''.format(lookUp, dbName, schemaName, tableName, lookUp)
    query_conn = pyodbc.connect(db_connQuery)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchone()
    query_cursor.close()
    query_conn.close()
        
    lastUpdated = db_return[0]

    return(lastUpdated)

def altLastModified(dbInstance, dbName, schemaName, tableName, lookUp):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    db_connQuery = getqueryDBConn(dbInstance, dbName)

    query_string = '''

    select [Date_Created] from [{}].[AdminGTS].[View_Layer_Table_History]
    where [Schema] = '{}' and [Table_Name] = '{}'

    '''.format(dbName, schemaName, tableName)

    query_conn = pyodbc.connect(db_connQuery)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchone()
    query_cursor.close()
    query_conn.close()
        
    lastUpdated = db_return[0]

    return(lastUpdated)

def altRecordCount(dbInstance, dbName, schemaName, tableName):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    db_connQuery = getqueryDBConn(dbInstance, dbName)

    query_string = '''

    select count(*) from {}.{}.{}

    '''.format(lookUp, dbName, schemaName, tableName, lookUp)
    query_conn = pyodbc.connect(db_connQuery)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchone()
    query_cursor.close()
    query_conn.close()
        
    totalCount = db_return[0]

    return(totalCount)

def sendfieldsToDBStore(dbName, schemaName, tableName, fieldsList):
#-------------------------------------------------------------------------------
# Name:        Function - captureGISDB
# Purpose:  Captures data about GIS datasets.
#-------------------------------------------------------------------------------

    query_string = '''

    select [GlobalID] from [dbo].[META_DataSets] where
        [DBName] = '{}' and
        [Schema] = '{}' and
        [Table] = '{}'

    '''.format(dbName, schemaName, tableName)

    query_conn = pyodbc.connect(db_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchone()
    query_cursor.close()
    query_conn.close()
        
    tableFK = db_return[0]

    for fieldFC in fieldsList:
        fname = '{}'.format(fieldFC[0])
        falias = '{}'.format(fieldFC[1])
        falias = falias.replace("'", "''")
        flength = fieldFC[2]
        ftype = '{}'.format(fieldFC[3])

        conn = pyodbc.connect(db_conn)
        cursor = conn.cursor()

        sqlCommand = '''

        insert into [dbo].[META_DataSetsFields] (
        [FieldName]
        , [FieldAlias]
        , [FieldLength]
        , [FieldType]
        , [SysCaptureDate]
        , [FkID]
        , [GlobalID]
        )
            Values ('{}', '{}', {}, '{}', getdate(), '{}', newid())

        '''.format(fname, falias, flength, ftype, tableFK)

        #print (sqlCommand)

        cursor.execute(sqlCommand)
        conn.commit()
        conn.close()

    return


#-------------------------------------------------------------------------------
#
#
#                                 MAIN SCRIPT
#
#
#-------------------------------------------------------------------------------

print ('***** Starting.....')

if __name__ == '__main__':
    main()

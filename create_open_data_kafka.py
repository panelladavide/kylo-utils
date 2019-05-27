from impala.dbapi import connect
from hdfs.ext.kerberos import KerberosClient
import re
import sys
import json
import configparser

def clean_characters(input):
    try:
        input = str(input)
        input = re.sub('[!$%^&*()+~=`{}[\]<>?\"\'./\\\]', r'', input)
        input = ' '.join(input.split())
        input = input.replace("à", "a").replace("é", "e").replace("è", "e").replace("ì", "i") \
            .replace("ò", "o").replace("ù", "u").replace("-", "_").replace(" ", "_") \
            .replace(",\n", ",_unknown_col\n").replace(";\n", ";_unknown_col\n").replace(":\n", ":_unknown_col\n") \
            .replace("|\n", "|_unknown_col\n")
        input = re.sub('[^\w;,:\|\n]', r'', input)

        return str(input)
    except Exception as e:
        print('{"failure": "true",')
        print('"failure_reason": "Unable to do data cleansing, caused by: ' + str(e) + '"}')
        exit()

#questa funzione si connette al servizio Impala
def impala_connection():
    try:
        conn = connect(host='192.168.30.24', port=21050, auth_mechanism='PLAIN', database='default', user=USER, password=PW, use_ssl=True)
        cursor = conn.cursor()
        return cursor
    except Exception as e:
        print('{"failure": "true",')
        print('"failure_reason": "Unable to connect to impala service, caused by: ' + str(e) + '"}')
        exit()

def create_db(cursor):
    try:
        ddl = 'CREATE DATABASE IF NOT EXISTS ' + database_name + ';'
        cursor.execute(ddl)
    except Exception as e:
        print('{"failure": "true",')
        print('"failure_reason": "Unable to create database `' + database_name + '`, caused by: ' + str(e) + '"}')
        exit()

def get_columns():
    columns = "(\n"
    try:
        if separator == "," or ";" or "|" or ":":
            for field in fields:
                column = clean_characters(str(field['name']))
                columns += "`" + column + "`" + " " + str(field['type']).upper() + ",\n"
            columns = columns[:-2]
            columns += "\n)"
            return columns
        else:
            print('{"failure": "true",')
            print('"failure_reason": "Separator: `' + str(separator) + '` not recognized"}')
            exit()
    except Exception as e:
        print('{"failure": "true",')
        print('"failure_reason": "Unable to extract columns, caused by: ' + str(e) + '"}')
        exit()

def create_table(cursor):
    try:
        ddl = 'CREATE EXTERNAL TABLE IF NOT EXISTS ' + str(database_name).lower() + "." + "`" + str(
            table_name + "_" + progressive_number).lower() + "`" + "\n" + columns + '\nROW FORMAT DELIMITED FIELDS TERMINATED BY \'' + separator + '\' STORED AS TEXTFILE\nLOCATION \'' + HDFS_PATH + '\' TBLPROPERTIES ("skip.header.line.count"="1");'
        cursor.execute(ddl)
    except Exception as e:
        print('{"failure": "true",')
        print('"failure_reason": "Unable to create table `' + database_name + "`.`" + table_name + '`, caused by: ' + str(e) + '"}')
        exit()


HDFS_CLIENT = KerberosClient('https://master.platform.daf.gov.it:50470')
config = configparser.ConfigParser()
config.read(".config_test")
USER = config.get("configuration", "username")
PW = config.get("configuration", "password")

input1 = {"id": "451","author": "parco_salento_default_admin", "organization": "parco_salento", "dataSetName": "ECON__policy", "separator": ",",  "resourceName": "barbieri_e_affini", "columns": [{"name": "Denominazione", "type": "String"}, {"name": "Tipologia", "type": "String"},{"name": "ubicazione", "type": "String"},{"name": "lat", "type": "String"},{"name": "lon", "type": "String"}]}
# input1 = json.loads(sys.stdin.read())

author = str(input1['author'])
organization = str(input1['organization'])
database_name = str(input1['dataSetName'])
table_name = str(input1['resourceName'])
progressive_number = str(input1['id'])
HDFS_PATH = "/user/daf/open_data/" + organization + "/" + database_name + "/" + table_name
fields = input1['columns']
separator = str(input1['separator'])

connection_cursor = impala_connection()

create_db(connection_cursor)

columns = get_columns()

create_table(connection_cursor)

print('{"failure": "false",')
print('"failure_reason": "NONE"}')
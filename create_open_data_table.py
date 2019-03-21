"""
Script di creazione della tabella degli opendata:
Input1: path
Input2: nome del dataset
Output:if not exists
        DB,tabella parquet, role db e role tb,
"""
from impala.dbapi import connect
from hdfs.ext.kerberos import KerberosClient
import sys
import re
import json
import requests
import configparser

print("")
print("dataset_name: " + sys.argv[1])
print("domain__subdomain " + sys.argv[2])
print("")

#questa funzione si connette al servizio Impala
def impala_conn(usr, pswd):
    print("Connect to impala service...")
    print("...")
    try:
        conn = connect(host='slave10-test', port=21050, auth_mechanism='PLAIN', database='default', user=usr, password=pswd, use_ssl=True)
        cursor = conn.cursor()
        print("Connection Established!")
        print(" ")
        return cursor
    except:
        print("Not connected!!!")


def create_db(cursor, db_name):
    print()
    ddl = 'CREATE' + ' DATABASE IF NOT EXISTS ' + str(db_name).lower() + ';'
    print(ddl)
    cursor.execute(ddl)
    # return


def get_columns(usr,pswd,dataset_name):
    # url = "http://catalog-manager.default.svc.cluster.local:9000/catalog-manager/v1/catalog-ds/getbyname/"+ dataset_name
    url = "http://127.0.0.1:9000/catalog-manager/v1/catalog-ds/getbyname/" + dataset_name
    print(url)
    r = requests.get(url, auth=(usr+str("@daf.gov.it"), pswd))
    if r.status_code >= 400:
        print("Response ERROR code: " + str(r.status_code))
    else:
        res = r.json()
        columns = []
        kylo = res['dataschema']['kyloSchema']
        print(kylo)
        kyloSchema = json.loads(kylo)
        table_info = kyloSchema['hiveFormat']\
            .replace("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'","")\
            .replace("\n","")\
            .replace(" WITH SERDEPROPERTIES ( ", "")\
            .replace(") STORED AS TEXTFILE","")
        fields = kyloSchema['fields']
        
        m = re.search('((\\\)(\;)|(\,)|(\:)|(\|))', table_info)
        sep = str(m.group(1)).replace("\\","")
        print("separatore inferito: \"" + sep+"\"")
        dict()
        if sep == "," or ";" or "|" or ":":
            for item in fields:
                mod_content = re.sub('[!$%^&*()+~=`{}[\]<>?\"\'./\\\]', r'', str(item['name']))
                mod_content1 = re.sub('[à]', r'a', str(mod_content))
                mod_content1 = re.sub('[èé]', r'e', str(mod_content1))
                mod_content1 = re.sub('[ì]', r'i', str(mod_content1))
                mod_content1 = re.sub('[ò]', r'o', str(mod_content1))
                mod_content1 = re.sub('[ù]', r'u', str(mod_content1))
                mod_content1 = re.sub('[-]', r'_', str(mod_content1))
                mod_content2 = re.sub('[ ]', r'_', str(mod_content1))
                mod_content2 = re.sub('[^\w;,:\|\n]', r'', str(mod_content2))
                mod_content2 = re.sub('(,\n)', r',_unknown_col\n', str(mod_content2))
                mod_content2 = re.sub('(;\n)', r';_unknown_col\n', str(mod_content2))
                mod_content2 = re.sub('(:\n)', r':_unknown_col\n', str(mod_content2))
                mod_content2 = re.sub('(\|\n)', r'|_unknown_col\n', str(mod_content2))
                col_name = "`" + str(mod_content2).lower() + "`"
                col_type = str(item['type']).upper()
                columns.append(col_name + " " + col_type)
            for i in range(0, len(fields) - 1):
                columns[i] = str(columns[i] + ",")
            return ' '.join(columns), sep
        else:
            print("...caricato il dataset su hdfs...")
            print("...sepatatore non riconosciuto...")
            print("...tabella non creata...")
            print("...fine dello script python.")
            exit()


def create_tbl(cursor, db_name, tb_name, tb_columns, delimiter):
    print("\nSend DDL to Impala:\n")
    columns = "(" + tb_columns + ")"
    ddl = 'CREATE EXTERNAL TABLE IF NOT EXISTS ' +str(db_name).lower() + "." + "`" + str(tb_name).lower() + "`" +  "\n" + \
        columns +'\nROW FORMAT DELIMITED FIELDS TERMINATED BY \'' + delimiter + '\' STORED AS TEXTFILE\nLOCATION \'' + HDFS_PATH + '\' TBLPROPERTIES ("skip.header.line.count"="1");'
    print(ddl)
    cursor.execute(ddl)


HDFS_CLIENT = KerberosClient('https://master.platform.daf.gov.it:50470')
config = configparser.ConfigParser()
config.read(".config_test")
TBL_NAME = str(sys.argv[1]) #.split('.')
DOMAIN = str(sys.argv[2]).split('__')[0]
USER = config.get("configuration", "username")
PW = config.get("configuration", "password")
HDFS_PATH = '/daf/opendata/' + TBL_NAME
DB_NAME = str("opendata__" + DOMAIN.lower())
print("HDFS_PATH: " + HDFS_PATH)
print("DB_NAME: " + DB_NAME)
print("TBL_NAME: " + TBL_NAME)


# Creo la connessione al servizio impala
cur = impala_conn(USER, PW)
# recupero i metadati dal catalog manager
f, s = get_columns(USER, PW, TBL_NAME)
# creo il db se non esiste
create_db(cur, DB_NAME)
# # creo la tabella su impala
create_tbl(cur, DB_NAME, TBL_NAME, f, s)

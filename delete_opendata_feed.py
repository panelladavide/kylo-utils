"""
Script di cancellazione della tabella degli opendata:
dopo aver preso da kafka in input un json contenente le informazioni necessarie
si procede alla cancellazione brutale della tabella da impala

"""
from impala.dbapi import connect
from hdfs.ext.kerberos import KerberosClient
import sys
import configparser

#questa funzione si connette al servizio Impala
def impala_conn(usr, pswd):
    try:
        conn = connect(host='192.168.30.24', port=21050, auth_mechanism='PLAIN', database='default', user=usr, password=pswd, use_ssl=True)
        cursor = conn.cursor()
        return cursor
    except Exception as e:
        print('{"failure": "true", "failure_reason": "Unable to connect to impala service, caused by:' + str(e) + '"}')
        exit()

#questa funzione effettua la cancellazione fisica della tabella da Impala
def delete_table(table_name, cursor):
    try:
        ddl = "DROP TABLE IF EXISTS " + table_name
        cursor.execute(ddl)
    except Exception as e:
        print('{"failure": "true", "failure_reason": "Unable to delete the table, caused by:' + str(e) + '"}')
        exit()

HDFS_CLIENT = KerberosClient('https://master.platform.daf.gov.it:50470')
config = configparser.ConfigParser()
config.read(".config_test")
USER = config.get("configuration", "username")
PW = config.get("configuration", "password")

TABLE_TO_DROP = sys.argv[1]
# TABLE_TO_DROP = "educ__istruzione.popolazione_scolastica_2016_2017_popolazione_scolastica_233"

# connessione ad Impala
cur = impala_conn(USER, PW)
# cancellazione tabella
delete_table(TABLE_TO_DROP, cur)

print('{"failure": "false", "failure_reason": "NONE"}')
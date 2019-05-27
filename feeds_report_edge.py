"""
Servizio per recuperare informazioni, creare un report.csv sui feed attualmente in produzione e
caricarlo sul hdfs di produzione:

"""
import requests, configparser, csv, time

config = configparser.ConfigParser()
config.read(".config")
# url constants
BASE_URL = "http://tba-kylo-services.default.svc.cluster.local:8420/api/v1"
FEEDS_URL = "http://tba-kylo-services.default.svc.cluster.local:8420/api/v1/feedmgr/feeds"

FAIL_URL = BASE_URL + '/v1/jobs'
HEADER = {'Accept': 'application/json'}
HEADER2 = {'Accept': 'application/json', 'Content-type': 'application/json'}
KUSER = config.get("configuration", "username")
KPW = config.get("configuration", "password")
USER = config.get("configuration", "daf_username")
PW = config.get("configuration", "daf_password")


res = requests.get(FEEDS_URL, headers=HEADER, auth=(KUSER, KPW))
feeds = res.json()['data']
active = []
row = []
tmp = []
ts = str(int(time.time()))
ts2 = str(int(time.time()))+'000'
for feed in feeds:
    if feed['active']:
        active.append(feed)
        r = requests.get(FEEDS_URL+'/'+feed['id'], headers=HEADER, auth=(KUSER, KPW))
        h = requests.get(BASE_URL+'/feeds/health-count/'+feed['categoryAndFeedSystemName'], headers=HEADER, auth=(KUSER, KPW))
        health = h.json()
        detail = r.json()
        if health['lastOpFeed'] is None or health['lastOpFeedState'] == 'RUNNING':
            tmp = [detail['id'], detail['categoryAndFeedDisplayName'], str(detail['templateName']).replace(' Ingest',''),
                   detail['schedule']['schedulingStrategy'], detail['schedule']['schedulingPeriod'],
                   detail['createDate'],
                   '', '', '', '', health['lastOpFeedState'], ts2
                   ]
        else:
            tmp = [detail['id'], detail['categoryAndFeedDisplayName'], str(detail['templateName']).replace(' Ingest',''),
                   detail['schedule']['schedulingStrategy'], detail['schedule']['schedulingPeriod'],detail['createDate'],
                   health['lastOpFeed']['status'], health['lastOpFeed']['exitCode'],
                   health['lastOpFeed']['startTime'], health['lastOpFeed']['endTime'],
                   health['lastOpFeedState'], ts2
                   ]
        row.append(tmp)
fields = [['id', 'nome', 'tipo_Feed', 'scheduler', 'frequenza', 'create_time', 'job_state', 'job_exit_code',
           'job_start_time_tms', 'job_end_time_tms', 'last_ops_feed_state', 'snapshot_tms']]
genericUrl = 'https://api.daf.teamdigitale.it/hdfs/proxy/uploads/d_ale/TECH/monitoraggio/kylo_report/'
url = genericUrl + "kylo_report_" + ts + ".csv?op=CREATE"
with open('/report.csv', 'w+') as csvFile:
    writer = csv.writer(csvFile)
    writer.writerows(fields)
    writer.writerows(row)
    csvFile.seek(0)
    payload = csvFile.read()
    csvFile.seek(0)
    headers = {"content-type": "text/csv"}
    response = requests.put(url,
                            data=payload, verify=False,
                            headers={'Accept': '*/*', 'content-type': 'text/csv'},
                            auth=(USER, PW)
                            )
csvFile.close()
print(response.status_code)

"""
Servizio per recuperare informazioni e scrivere un report sui feed attualmente in produzione:

"""
import requests, configparser, csv, time, math
from cron_descriptor import get_description

config = configparser.ConfigParser()
config.read(".config")
# url constants
BASE_URL = "http://127.0.0.1:8400/proxy/v1"
FEEDS_URL = "http://127.0.0.1:8400/proxy/v1/feedmgr/feeds"

FAIL_URL = BASE_URL + '/v1/jobs'
HEADER = {'Accept': 'application/json'}
HEADER2 = {'Accept': 'application/json', 'Content-type': 'application/json'}
KUSER = config.get("configuration", "username")
KPW = config.get("configuration", "password")


res = requests.get(FEEDS_URL, headers=HEADER, auth=(KUSER, KPW))
feeds = res.json()['data']
print(len(feeds))
active = []
row = []
tmp = []
i = 0
for feed in feeds:
    i += 1
    print(str(round(i/len(feeds)*100, 1))+'%   '+feed['categoryAndFeedSystemName'])
    if feed['active']:
        active.append(feed)
        r = requests.get(FEEDS_URL+'/'+feed['id'], headers=HEADER, auth=(KUSER, KPW))
        h = requests.get(BASE_URL+'/feeds/health-count/'+feed['categoryAndFeedSystemName'], headers=HEADER, auth=(KUSER, KPW))
        health = h.json()
        detail = r.json()
        if detail['schedule']['schedulingStrategy'] == 'CRON_DRIVEN':
            cron_expression = get_description(detail['schedule']['schedulingPeriod'])
        else:
            cron_expression = detail['schedule']['schedulingPeriod']
        if health['lastOpFeed'] is None or health['lastOpFeedState'] == 'RUNNING':
            tmp = [detail['id'], detail['categoryAndFeedDisplayName'], detail['state'], str(detail['templateName']).replace(' Ingest',''),
                   detail['schedule']['schedulingStrategy'], cron_expression,
                   time.strftime('%d/%m/%Y %H:%M:%S',time.gmtime(detail['createDate']/1000.)),
                   '', '', '', '','','',
                   health['lastOpFeedState']
                   ]
        else:
            tmp = [detail['id'], detail['categoryAndFeedDisplayName'], detail['state'], str(detail['templateName']).replace(' Ingest',''),
                   detail['schedule']['schedulingStrategy'], cron_expression,
                   time.strftime('%d/%m/%Y %H:%M:%S', time.gmtime(detail['createDate']/1000.)),
                   health['lastOpFeed']['status'], health['lastOpFeed']['exitCode'],
                   time.strftime('%d/%m/%Y %H:%M:%S', time.gmtime(health['lastOpFeed']['startTime']/1000.)),health['lastOpFeed']['startTime'],
                   time.strftime('%d/%m/%Y %H:%M:%S', time.gmtime(health['lastOpFeed']['endTime']/1000.)),health['lastOpFeed']['endTime'],
                   health['lastOpFeedState']
                   ]
        row.append(tmp)
print(row)
fields = [['id', 'nome', 'state', 'tipo Feed', 'scheduler', 'frequenza', 'create time', 'job state', 'job exit code',
           'job start time', 'job start time tms', 'job end time', 'job end time tms', 'last ops feed state']]
with open('report.csv', 'w') as csvFile:
    writer = csv.writer(csvFile)
    writer.writerows(fields)
    writer.writerows(row)

csvFile.close()


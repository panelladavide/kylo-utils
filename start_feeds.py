"""
Servizio per lo startNow dei feeds su kylo:

"""
import requests
import configparser
config = configparser.ConfigParser()
config.read(".config")
KUSER = config.get("configuration", "username")
KPW = config.get("configuration", "password")

FEEDS_URL = "http://127.0.0.1:8400/proxy/v1/feedmgr/feeds/by-name/"
START_URL = 'http://127.0.0.1:8400/proxy/v1/feedmgr/feeds/start/'
HEADER = {'Accept': 'application/json'}
HEADER2 = {'Accept': 'application/json', 'Content-type': 'application/json'}
KUSER = config.get("configuration", "username")
KPW = config.get("configuration", "password")
feeds = []
for i in range(200, 205):
    res = requests.get(FEEDS_URL+"test.test_persisted_provenance_"+str(i), headers=HEADER2, auth=(KUSER, KPW))
    response = res.json()
    print("\n"+str(i))
    print(response['systemFeedName'])
    print(response['feedId'])
    feeds.append(response['feedId'])

print("list of feed Ids ")
print(feeds)
for f in range(1, len(feeds)):
    print(f)
    print("starting feeds: " + feeds[f])
    r = requests.post(START_URL+feeds[f], headers=HEADER2, auth=(KUSER, KPW))
    print(r.status_code)

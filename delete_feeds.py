"""
Servizio di cancellazione dei feeds kylo:
Input:
"""


import requests
import time
import configparser

config = configparser.ConfigParser()
config.read(".config")
start = time.time()
FEEDS_BY_NAME_URL = "http://127.0.0.1:8400/proxy/v1/feedmgr/feeds/by-name/"
FEEDS_URL = "http://127.0.0.1:8400/proxy/v1/feedmgr/feeds/"
HEADER = {'Accept': 'application/json'}
HEADER2 = {'Accept': 'application/json', 'Content-type': 'application/json'}
KUSER = config.get("configuration", "username")
KPW = config.get("configuration", "password")


feeds_id = []
feeds_name = []
feeds_not_deleted = []
feeds_deleted = []
l = [2, 3]
for i in range(400, 420):
    print(i)
    res = requests.get(FEEDS_BY_NAME_URL+"test.test_persisted_provenance_"+str(i), headers=HEADER2, auth=(KUSER, KPW))
    print(res.status_code)
    print(str(res.content))
    if str("b''") not in str(res.content):
        response = res.json()
        print(str(i))
        print(response['systemFeedName'])
        print(response['feedId'])
        feeds_name.append(response['systemFeedName'])
        feeds_id.append(response['feedId'])
        print("deleting feed: " + str(response['systemFeedName']))
        print("url: " + str(FEEDS_URL) + str(response['feedId']))
        r = requests.delete(FEEDS_URL + response['feedId'], headers=HEADER2, auth=(KUSER, KPW))
        print("il feed " + response['systemFeedName'] + " ha dato risposta: ")
        print(r.status_code)
        print(r.content)
        if r.status_code == 204:
            print("il feed " + response['systemFeedName'] + " e' stato cancellato")
            feeds_deleted.append(response['feedId'])
        else:
            feeds_not_deleted.append(response['feedId'])

    else:
        if str("b''") in str(res.content):
            pass
end = time.time()
t = end - start
print("tempo di esecuzione: " + str(t))

print("feeds cancellati: " + str(feeds_deleted))
print("feeds non cancellati: " + str(feeds_not_deleted))

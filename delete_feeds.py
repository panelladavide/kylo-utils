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
for i in range(502, 520):
    print(i)
    res = requests.get(FEEDS_BY_NAME_URL+"test.test_persisted_provenance_"+str(i), headers=HEADER2, auth=(KUSER, KPW))
    print(res.status_code)
    print(res.content)
    if res.status_code == 200:
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
        pass
# print("lenfeeds_id: "+str(len(feeds_name)))
#
# for f in range(0, len(feeds_name)):
#     print(f)
#     print("deleting feed: " + feeds_name[f])
#     print("url: " + str(FEEDS_URL)+str(feeds_id[f]))
#     r = requests.delete(FEEDS_URL+feeds_id[f], headers=HEADER2, auth=(KUSER, KPW))
#     print("il feed " + feeds_name[f] + " ha dato risposta: ")
#     print(r.status_code)
#     print(r.content)
end = time.time()
t = end - start
print("tempo di esecuzione: " + str(t))

print("feeds cancellati: " + feeds_deleted)
print("feeds non cancellati: " + feeds_not_deleted)

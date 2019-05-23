"""
Servizio che disabilita e cancella i feeds kylo:
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
GET_URL = 'http://127.0.0.1:8400/proxy/v1/feedmgr/feeds?filter=test'
DISABLE_URL = 'http://localhost:8400/proxy/v1/feedmgr/feeds/disable/'
HEADER = {'Accept': 'application/json'}
HEADER2 = {'Accept': 'application/json', 'Content-type': 'application/json'}
KUSER = config.get("configuration", "username")
KPW = config.get("configuration", "password")


feeds_id = []
feeds_name = []
feeds_not_deleted = []
feeds_deleted = []
feeds_to_disable = []
res_dis = requests.get(GET_URL, headers=HEADER2, auth=(KUSER, KPW))
feeds_filtered = res_dis.json()['data']
print(feeds_filtered)
print(len(feeds_filtered))
for feed in feeds_filtered:
    if feed['categoryAndFeedSystemName'] not in ['test.kafka1','pac_inapp.pac_inapp_o_t456_24date_50r_test','pac_inapp.pac_inapp_o_t456_24date_50r_test2','regione_toscana.regione_toscana_o_monitoraggio_test_finale_cri','regione_umbria.regione_umbria_o_mad_progetti_economics_totali_greatest','regione_umbria.regione_umbria_o_out_mad_progetti_economics_totali_greatest','regione_umbria.regione_umbria_o_test_indicatori','regione_umbria.regione_umbria_o_test_pdrt_all_a','daf_data.test_cciss']:
        print("disabilito il feed: "+feed['categoryAndFeedSystemName'])
        feeds_to_disable.append(feed['feedId'])
        disable = requests.post(DISABLE_URL+str(feed['feedId']), headers=HEADER2, auth=(KUSER, KPW))
        print(disable.status_code)
        print("cancello il feed: " + str(feed['systemFeedName']))
        print("url: " + str(FEEDS_URL) + str(feed['feedId']))
        r = requests.delete(FEEDS_URL + feed['feedId'], headers=HEADER2, auth=(KUSER, KPW))
        print("il feed " + feed['systemFeedName'] + " ha dato risposta: ")
        print(r.status_code)
        print(r.content)
        if r.status_code == 204:
            print("il feed " + feed['systemFeedName'] + " e' stato cancellato")
            feeds_deleted.append(feed['feedId'])
        else:
            feeds_not_deleted.append(feed['feedId'])
end = time.time()
t = end - start
print("tempo di esecuzione: " + str(t))

print("feeds cancellati: " + str(feeds_deleted))
print("feeds non cancellati: " + str(feeds_not_deleted))

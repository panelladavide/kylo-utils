import json
"""
Servizio per la creazione dei Feeds su kylo:

per creare il json del payload esegui questa riga sul terminale
for i in {1..10}; do sed 's/test_template/test_template_'${i}'/g' test_template.json > create_feeds/test_template${i}.json; done
"""
import requests
import configparser
config = configparser.ConfigParser()
config.read(".config")

BASE_URL = "http://127.0.0.1:8400/proxy"
JOBS_URL = "http://127.0.0.1:8400/proxy/v1/jobs/running?limit=500&start=0"
FEEDS_URL = "http://127.0.0.1:8400/proxy/v1/feedmgr/feeds"
FAIL_URL = BASE_URL + '/v1/jobs'
HEADER = {'Accept': 'application/json'}
HEADER2 = {'Accept': 'application/json', 'Content-type': 'application/json'}
KUSER = config.get("configuration", "username")
KPW = config.get("configuration", "password")

for i in range(1, 10):
    print(i)
    with open("/home/davide/PycharmProjects/script_ddl/create_feeds/test_template" + str(i) + ".json") as json_data:
        d = json.load(json_data)
        print(d)
        # new_json = str(d).replace("test_persisted_provenance", "test_persisted_provenance_" + str(i))
        # print(new_json)

    res = requests.post(FEEDS_URL, headers=HEADER, auth=(KUSER, KPW), json=d)
    print(res.status_code)
    print(res.json())
    json_data.close()

"""
Servizio per il fail dei job in running su kylo:

"""
import requests
import configparser
config = configparser.ConfigParser()
config.read(".config")
# url constants
BASE_URL = "http://127.0.0.1:8400/proxy"
JOBS_URL = "http://127.0.0.1:8400/proxy/v1/jobs/running?limit=500&start=0"
FAIL_URL = BASE_URL + '/v1/jobs'
HEADER = {'Accept': 'application/json'}
HEADER2 = {'Accept': 'application/json', 'Content-type': 'application/json'}
KUSER = config.get("configuration", "username")
KPW = config.get("configuration", "password")


def get_running_id(url, header):
    by_name = str(url)+'by-name/'
    try:
        print("chiamata a kylo: ")
        res = requests.get(url, headers=header, auth=(KUSER, KPW))
        print(res.status_code)
    except:
        print(res.json()['message'])
        f_id = None
    return f_id


res = requests.get(JOBS_URL, headers=HEADER, auth=(KUSER, KPW))
print(res)
jobs = res.json()['data']
print(jobs)
for job in jobs:
    print(job['feedName'])
    if job['feedName'] == 'test.test_persisted_provenance':
        print(job['executionId'])
        fail_id_url = FAIL_URL+'/'+str(job['executionId'])+'/fail'
        print(fail_id_url)
        res = requests.post(fail_id_url, headers=HEADER, auth=(KUSER, KPW))
    else:
        print("non pac_anac.pac_anac_o_anac_pub_simog:\n")
        print(job['feedName'])
        print("feed rimane in running")
print(res.json())

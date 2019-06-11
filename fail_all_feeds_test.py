"""
Servizio per il fail su test dei job in running su kylo:

"""
import requests
import configparser
config = configparser.ConfigParser()
config.read(".config")

# url constants
BASE_URL = "http://127.0.0.1:8400/proxy"
JOBS_URL = BASE_URL + "/v1/jobs/running?limit=600&start=0"
FAIL_URL = BASE_URL + '/v1/jobs'
GET_FALED_URL ="/v1/jobs/failed"
HEADER = {'Accept': 'application/json'}
HEADER2 = {'Accept': 'application/json', 'Content-type': 'application/json'}
KUSER = config.get("configuration", "username")
KPW = config.get("configuration", "passwordTest")



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


res = requests.get(JOBS_URL, headers=HEADER2, auth=(KUSER, KPW))
jobs = res.json()['data']
print(jobs)
for job in jobs:
    print(job['feedName'])
    print(job['executionId'])
    abandon_id_url= "http://127.0.0.1:8400/proxy/v1/jobs/"+str(job['executionId'])+"/abandon"
    print(abandon_id_url)
    res = requests.post(abandon_id_url, headers=HEADER, auth=(KUSER, KPW))
    print("feed "+job['feedName']+" abbandonato")
requests.post('http://127.0.0.1:8400/proxy/v1/jobs/977/fail', HEADER, auth=(KUSER, KPW))
print(res.json())

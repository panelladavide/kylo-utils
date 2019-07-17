import requests
import  sys

def nifi_bullettin_request():
    url = "http://tba-nifi.default.svc.cluster.local:8080/nifi-api/flow/bulletin-board"
    r = requests.get(url)
    status_code = r.status_code
    if status_code >= 400:
        print("Response ERROR code: " + str(status_code))
    else:
        r = r.json()
        bulletins = r['bulletinBoard']['bulletins']
        if bulletins:
            output = [x for x in bulletins if x['bulletin']['level'] == 'ERROR' and uuid_to_match in str(x['bulletin']['message'])]
            # print("Status code: " + str(status_code))
            # length = int(len("Status code: ") + len(str(status_code)))
            for item in output:
                print(str(item['bulletin']['message'])[:(str_length)] + '\n')

str_length = int(sys.argv[1])
uuid_to_match = str(sys.argv[2])

nifi_bullettin_request()
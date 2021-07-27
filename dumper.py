import json
import requests


def dump_data():
    f = open("outputdata.txt", "r")
    f = f.read()

    url = 'http://127.0.0.1:8080/analytics/dump_data/object'
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

    for i in eval(f):
        data = json.dumps(i)
        x = requests.post(url, data = data, headers=headers)
        print(x.status_code)
        print(x.text)

dump_data()
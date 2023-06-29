import json
import requests
from time import sleep

max_retry = 3


def quickwit_ingest(index, data_list, retry=0):
    try:
        res = requests.post(f'http://localhost:7280/api/v1/{index}/ingest', data=__jsonify_data(data_list, index))
    except requests.exceptions.ConnectionError as e:
        retry += 1
        assert retry <= max_retry, f'[ENDPOINT CONNECTION FAIL] Failed to connect to endpoint http://localhost:7280/api/v1/{index}/ingest\n{e}\n'
        sleep(2*retry)
        print(f"[ENDPOINT ERROR] Failed to connect to endpoint http://localhost:7280/api/v1/{index}/ingest, retrying in {2*retry} seconds..\n")
        return quickwit_ingest(index, data_list, retry=retry)
    return res


def __jsonify_data(data_list, msg_type):
    res = list()
    i = 0
    for data in data_list:
        if msg_type == 'fetchevent':
            try:
                _tmp = data['request']
                if _tmp != '':
                    data['request'] = json.loads(_tmp)
                else:
                    data['request'] = {}
                _tmp = data['response']
                if _tmp != '':
                    data['response'] = json.loads(_tmp)
                    if data['response']['body'][:1] == '{' or data['response']['body'][:2] == '[{':
                        data['response']['body'] = json.loads(data['response']['body'])
                else:
                    data['response'] = {}
            except Exception as e:
                print(f'Error {e}\tWhile decoding fetchevent\nEvent: {data}\n')
        elif msg_type == 'graphql':
            try:
                _tmp = data['variables']
                if _tmp != '':
                    data['variables'] = json.loads(_tmp)
                else:
                    data['variables'] = {}
                _tmp = data['response']
                if _tmp != '':
                    data['response'] = json.loads(_tmp)
                else:
                    data['response'] = {}
            except Exception as e:
                print(f'Error {e}\tWhile decoding graphql\nEvent: {data}\n')
        i += 1
        res.append(json.dumps(data))
    return '\n'.join(res)

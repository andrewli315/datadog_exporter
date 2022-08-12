import coloredlogs
import datetime
import logging
import json
import requests
import sys
import os
import time
coloredlogs.install(level=logging.INFO)

api_key = os.environ.get('DD_API_KEY') or input('DD API Key:')
application_key = os.environ.get('DD_APP_KEY') or input('DD APP Key:')

query = sys.argv[1].replace('index:main ', '')

fout = open(sys.argv[2], 'w')


time_from = '2022-08-04T04:00:00Z'
time_to = '2022-08-04T06:53:53.000Z'

#time_from = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")

logging.info(f'{query=}, {time_from=} to {time_to=}')

query = {
    'limit': 1000,
    'query': query,
    'sort': 'desc',
    'time': {
        'from': time_from,
        'to': time_to,
    }
}

while True:
    response = requests.post(
        'https://api.datadoghq.com/api/v1/logs-queries/list',
        headers={
            'Content-Type': 'application/json',
            'DD-API-KEY': api_key,
            'DD-APPLICATION-KEY': application_key,
        },
        json=query
    )
    data = response.json()

    if not 'logs' in data:
        time.sleep(3600)
        continue

    logging.info(f'{query.get("startAt")} @ {data["logs"][0]["content"]["timestamp"]}')

    for row in data['logs']:
        fout.write(str(row['content']['timestamp']))
        fout.write(',')
        if( 'network' in row['content']['attributes'].keys()):
            fout.write(str(row['content']['attributes']['network']['client']['ip']))
        fout.write(',')
        fout.write(str(row['content']['message']))
        fout.write('\n')
        #print(json.dumps(row))
        #print(row['content']['attributes']['network']['client']['ip'])
        
        #print(json.dumps(row))
        
        #fout.write(row)
    fout.flush()
    if data['nextLogId']:
        query['startAt'] = data['nextLogId']
    else:       
        break


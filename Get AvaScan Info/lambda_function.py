# Online Python compiler (interpreter) to run Python online.
# Write Python 3 code in this online editor and run it.

import requests
import json
import time
from json import dumps
from kafka import KafkaProducer

def lambda_handler(event, context):
    
    api_url = "https://avascan.info/api/v1/home/statistics"
     
    headers =  {"Content-Type":"application/json"}
    response = requests.get(api_url, headers=headers)
    # print("response")
    # print(response)
    # print(response.status_code)
    jData = json.loads(response.content)
    # print(jData)
     
    messageData = {"messageType":"AvaScanInfo","blockchainType":"Avalanche","timestamp":time.time(),"body":jData}
    print(messageData)
    producer = KafkaProducer(bootstrap_servers=['13.58.200.246:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
    producer.send('avalanche-data', value=messageData)

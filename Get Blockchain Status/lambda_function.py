# Online Python compiler (interpreter) to run Python online.
# Write Python 3 code in this online editor and run it.

import requests
import json
import time
from json import dumps
from kafka import KafkaProducer
import pymysql

rds_host  = "database-2.csavgatwn8yx.us-east-2.rds.amazonaws.com"
name = "admin"
password = "abc12345"
db_name = "coindelta"
print("before connection")

conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name,port=3306, connect_timeout=5)

def lambda_handler(event, context):
    
    with conn.cursor() as cur:
        
        queryGet = """SELECT * FROM blockchain"""
        cur.execute(queryGet)
        validator_response = cur.fetchall()
        print("response of get Query")
        print(validator_response)

        conn.commit()

        for blockchain in validator_response:
            print(blockchain)
            api_url = "http://ec2-18-218-170-38.us-east-2.compute.amazonaws.com:9650/ext/P"
            todo = {"jsonrpc": "2.0", "method": "platform.getBlockchainStatus","id": 1, "params": {"blockchainID":"{}".format(blockchain[1]) }}
            headers =  {"Content-Type":"application/json"}
            response = requests.post(api_url, data=json.dumps(todo), headers=headers)
            print(response)
            print(response.status_code)
            jData = json.loads(response.content)
            print(jData)
            result= jData.get("result")
            mainResult = {'jsonrpc': jData.get("jsonrpc"), 'result':{"status": result.get("status"),"blockchainId": blockchain[1]}, 'id': jData.get("id")}
            print(mainResult)
            
            jData = mainResult
            print("Main jData")
            print(jData)
            messageData = {"messageType":"BlockchainStatus","blockchainType":"Avalanche","timestamp":time.time(),"body":jData}
            print("Message data")
            print(messageData)
            producer = KafkaProducer(bootstrap_servers=['13.58.200.246:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
            producer.send('avalanche-data', value=messageData)
    

lambda_handler("","")
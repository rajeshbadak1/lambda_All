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
conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name,port=3306, connect_timeout=5)

def lambda_handler(event, context):
    
     with conn.cursor() as cur:
        
        queryGet = """SELECT * FROM node_info"""
        cur.execute(queryGet)
        node_response = cur.fetchall()
        print("response of get Query")
        print(node_response)

        conn.commit()
    
        for node in node_response:

            api_url = "http://"+node[3]+":"+ str(node[5]) +"/ext/P"
            todo = {"jsonrpc": "2.0", "method": "platform.getCurrentValidators","params": { "nodeIDs": [node[4]]},"id": 1 }
            headers =  {"Content-Type":"application/json"}
            print(api_url)
            print(todo)
            response = requests.post(api_url, data=json.dumps(todo), headers=headers)
            print(response)
            print(response.status_code)
            jData = json.loads(response.content)
            print(jData)
            
            messageData = {"messageType":"Reward","blockchainType":"Avalanche","timestamp":time.time(),"body":jData}

            producer = KafkaProducer(bootstrap_servers=['13.58.200.246:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
            # producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
            producer.send('avalanche-data', value=messageData)
 
lambda_handler("","")
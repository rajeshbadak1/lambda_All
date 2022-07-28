# Online Python compiler (interpreter) to run Python online.
# Write Python 3 code in this online editor and run it.

import requests
import json
import time
from json import dumps
from datetime import datetime
from datetime import timedelta
import pymysql
from kafka import KafkaProducer

# rds_host = "database-2.csavgatwn8yx.us-east-2.rds.amazonaws.com"
# name = "admin"
# password = "abc12345"
# db_name = "coindelta"
rds_host = "localhost"
name = "root"
password = "pass"
db_name = "coindelta"
conn = pymysql.connect(host=rds_host, user=name, passwd=password,db=db_name, port=3306, connect_timeout=5)


def lambda_handler(event, context):
    with conn.cursor() as cur:

        queryGet = """SELECT * FROM blockchain"""
        cur.execute(queryGet)
        node_response = cur.fetchall()
        # print("response of get Query")
        # print(node_response)

        conn.commit()

        for blockchains in node_response:
            print(blockchains)

            chainId = blockchains[1]
            subnetId = blockchains[3]

            cur.execute("SELECT next FROM coindelta.transaction WHERE chainid=%s order by id desc limit 1", (chainId,))
            node_response1 = cur.fetchall()
            print("node_response1")
            print(node_response1)
            api_url = ""

            if node_response1 != ():
                # startTime = 0
                # try:
                #     date = node_response1[0][0]
                #     timestruct = datetime.strptime(str(date), '%Y-%m-%dT%H:%M:%S.%fZ')
                #     minusTime = timestruct + timedelta(days=0, hours=0, minutes=5, seconds=5)
                #     timestructMinus = time.strptime(str(minusTime), '%Y-%m-%d %H:%M:%S.%f')
                #     startTime = int(time.mktime(timestructMinus))
                # except:
                #     date = node_response1[0][0]
                #     timestruct = datetime.strptime(str(date), '%Y-%m-%dT%H:%M:%SZ')
                #     minusTime = timestruct + timedelta(days=0, hours=0, minutes=5, seconds=5)
                #     timestructMinus = time.strptime(str(minusTime), '%Y-%m-%d %H:%M:%S')
                #     startTime = int(time.mktime(timestructMinus))

                # print("startTime")
                # print(startTime)
                url = node_response1[0][0]
                print(url)
                api_url = "https://explorerapi.avax.network/v2/transactions?"+url
            else:
                api_url = "https://explorerapi.avax.network/v2/transactions?chainID=" + chainId + "&chainID=" + subnetId + "&sort=timestamp-asc&start=1&limit=5"

            print(api_url)

            headers = {"Content-Type": "application/json"}
            response = requests.get(api_url, headers=headers)
            jData = json.loads(response.content)
            print("jData.length")
            # print(len(jData))

            mainBody = {"transactions": jData.get("transactions"), "chainId": chainId, "next": jData.get("next")}
            if mainBody.get("transactions") != None:
                print(len(mainBody.get("transactions")))
            # result = jData.get("result")
            # print(mainBody)
            timestamp = time.time()
            # print(timestamp)
            messageData = {"messageType": "Transaction", "blockchainType": "Avalanche","timestamp": timestamp, "body": mainBody}
            # print("messageData")
            # print(messageData)
            # producer = KafkaProducer(bootstrap_servers=['18.116.180.51:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
            producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
            producer.send('avalanche-data', value=messageData)


lambda_handler("", "")

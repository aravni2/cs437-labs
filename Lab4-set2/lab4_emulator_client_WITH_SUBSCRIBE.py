# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np


#TODO 1: modify the following parameters
#Starting and end index, modify this

topics = 'vehicle/emission/data'
device_st = 0
device_end = 5

#Path to the dataset, modify this
data_path = "vehicle_data/vehicle{}.csv"

#Path to your certificates, modify this
certificate_formatter = "certs/cs437_Thing-{} cert.pem"
key_formatter = "private_keys/cs437_Thing-{} private.key"


class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        #ADDED to code to create separation of device id and device name
        self.device_name = f'cs437_Thing-{device_id}'
        self.device_id = str(device_id)
        self.state = 0
        self.client = AWSIoTMQTTClient(self.device_name)
        #TOD 2: modify your broker address
        self.client.configureEndpoint("apb8nhcywqwx-ats.iot.us-east-2.amazonaws.com", 8883)
        self.client.configureCredentials("certs_and_keys/AmazonRootCA1.pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5)  # 5 sec
        self.client.onMessage = self.customOnMessage
        

    def customOnMessage(self,message):
        #TOD 3: fill in the function to show your received message
        payload = message.payload
        decoded_payload=json.loads(payload.decode('utf-8'))
        print("client {} received payload {} from topic {}".format(self.device_name,decoded_payload,'vehicle/emission/data'))
        print (decoded_payload['status'])
        # print('DID THIS WORK!!!!!!!!!')


    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass


    def publish(self, iter, topic="vehicle/emission/data"):
    # Load the vehicle's emission data
        # print('inside publish',data_path.format(self.device_id))
        # df = pd.read_csv(data_path.format(self.device_id))
        print('inside publish, sending data for',data_path.format(iter))
        df = pd.read_csv(data_path.format(iter))
        jsonListofDict=[]
        for index, row in df.iterrows():
            # Create a JSON payload from the row data
            # payload = json.dumps(row.to_dict())
            dataTemp = row.to_dict()
            jsonListofDict.append(dataTemp)
            # Publish the payload to the specified topic
            # self.client.publishAsync(topic, payload, 0, ackCallback=self.customPubackCallback)
            # print(f"Publishing: {payload} to {topic}")

        finalPayload={
        "sdk_version": "0.1.4",
        "message_id": str(iter),
        "status": "200",
        "route": "MaxCO2",
        "message": jsonListofDict}
        # print ('vehicle'+str(iter), finalPayload)
        # print(f"Publishing: {jsonListofDict} to {topic} for Vehicle{self.device_id}")
        self.client.publishAsync(topic, json.dumps(finalPayload), 0, ackCallback=self.customPubackCallback)
            # Sleep to simulate real-time data publishings
            # time.sleep(1)
            
print("Loading vehicle data...")
data = []
for i in range(5):
    a = pd.read_csv(data_path.format(i))
    data.append(a)

print("Initializing MQTTClients...")
clients = []
for device_id in range(device_st,device_end):
    device_name = 'cs437_Thing-{}'
    print(device_id,certificate_formatter.format(device_id,device_id),key_formatter.format(device_id,device_id) )
    client = MQTTClient(device_id,certificate_formatter.format(device_id) ,key_formatter.format(device_id))
    client.client.connect()


    #TODO: CREATE SUBSCRIBEASYNC FOR TOPICS SPECIFIC TO EACH VEHICLE
    # print ('subbing for device: ', topics.format(device_id))
    print ("subscribing to:", "core/vehicle{}/MaxCO2".format(device_id))
    client.client.subscribeAsync("core/vehicle{}/MaxCO2".format(device_id),1,client.customSubackCallback)
    # client.client.subscribeAsync(topics,0,client.customSubackCallback)
    # client.s("vehicle/emission/data")
    clients.append(client)
    
 
print('current clients',clients)
while True:
    print("send now?")
    x = input()
    if x == "s":
        for c in clients:
            print ('client is: ', c)
            c.publish(clients.index(c))
            time.sleep(10)

    elif x == "d":
        for c in clients:
            c.client.disconnect()
        print("All devices disconnected")
        exit()
    else:
        print("wrong key pressed")

    time.sleep(3)






# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np


#TODO 1: modify the following parameters
#Starting and end index, modify this
device_st = 0
device_end = 5

#Path to the dataset, modify this
data_path = "vehicle_data/vehicle{}.csv"

#Path to your certificates, modify this
certificate_formatter = "certs/Lab4_IoT_Thing-{} cert.pem"
key_formatter = "private_keys/Lab4_IoT_Thing-{} private.key"


class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        #ADDED to code to create separation of device id and device name
        self.device_name = f'Lab4_IoT_Thing-{device_id}'
        self.device_id = str(device_id)

        self.state = 0
        self.client = AWSIoTMQTTClient(self.device_name)
        #TODO 2: modify your broker address
        self.client.configureEndpoint("a3s2af5ccebmkg-ats.iot.us-east-2.amazonaws.com", 8883)
        self.client.configureCredentials("Certs and Keys/AmazonRootCA1.pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5)  # 5 sec
        self.client.onMessage = self.customOnMessage
        

    def customOnMessage(self,message):
        #TODO 3: fill in the function to show your received message
        payload = {"message" : 'Hello!'}
        print("client {} received payload {} from topic {}".format(self.device_name,message ,'vehicle/emission/data' ))


    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass


    def publish(self, topic="vehicle/emission/data"):
    # Load the vehicle's emission data
        print('inside publish',data_path.format(self.device_id))
        df = pd.read_csv(data_path.format(self.device_id))
        for index, row in df.iterrows():
            # Create a JSON payload from the row data
            payload = json.dumps(row.to_dict())
            
            # Publish the payload to the specified topic
            print(f"Publishing: {payload} to {topic}")
            self.client.publishAsync(topic, payload, 0, ackCallback=self.customPubackCallback)
            
            # Sleep to simulate real-time data publishings
            # time.sleep(1)
            



print("Loading vehicle data...")
data = []
for i in range(5):
    a = pd.read_csv(data_path.format(i))
    data.append(a)

print("Initializing MQTTClients...")
clients = []
for device_id in range(device_st, device_end):
    device_name = 'Lab4_IoT_Thing-{}'
    print(device_id,certificate_formatter.format(device_id,device_id),key_formatter.format(device_id,device_id) )
    client = MQTTClient(device_id,certificate_formatter.format(device_id) ,key_formatter.format(device_id))
    client.client.connect()
    clients.append(client)
 
print('current clients',clients)
while True:
    print("send now?")
    x = input()
    if x == "s":
        for i,c in enumerate(clients):
            
            c.publish()

    elif x == "d":
        for c in clients:
            c.client.disconnect()
        print("All devices disconnected")
        exit()
    else:
        print("wrong key pressed")

    time.sleep(3)






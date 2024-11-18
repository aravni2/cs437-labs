#credentials
ACCESS_KEY = <fill in own access_key>
SECRET_KEY = <fill in own secret_key>

#endpoint
self.client.configureEndpoint("a3s2af5ccebmkg-ats.iot.us-east-2.amazonaws.com", 8883)

#rootCA
self.client.configureCredentials("Certs and Keys/AmazonRootCA1.pem", key, cert)

#cert and key
#for basic discovery, use each device.pem.crt for --cert and private.pem.key for --key
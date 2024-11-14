import boto3

import json
################################################### Create random name for things
import random
import string

################################################### Parameters for Thing
ACCESS_KEY = 'AKIAVVZPCGDPP3ATLYXA'
SECRET_KEY = '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'

thingArn = ''
thingId = ''
thingName = 'My_iot_Thing-'+''.join([random.choice(string.ascii_letters + string.digits) for n in range(15)])
defaultPolicyName = 'My_Iot_grp_Policy'
###################################################

def createThing():
  global thingClient
  thingResponse = thingClient.create_thing(
      thingName = thingName
  )
  data = json.loads(json.dumps(thingResponse, sort_keys=False, indent=4))
  for element in data: 
    if element == 'thingArn':
        thingArn = data['thingArn']
    elif element == 'thingId':
        thingId = data['thingId']
		
    createCertificate()
    add_thing_to_group()
	
def createCertificate():
	global thingClient
	certResponse = thingClient.create_keys_and_certificate(
			setAsActive = True
	)
	data = json.loads(json.dumps(certResponse, sort_keys=False, indent=4))
	for element in data: 
			if element == 'certificateArn':
					certificateArn = data['certificateArn']
			elif element == 'keyPair':
					PublicKey = data['keyPair']['PublicKey']
					PrivateKey = data['keyPair']['PrivateKey']
			elif element == 'certificatePem':
					certificatePem = data['certificatePem']
			elif element == 'certificateId':
					certificateId = data['certificateId']
							
	with open(f'public_keys/{thingName} public.key', 'w') as outfile:
			outfile.write(PublicKey)
	with open(f'private_keys/{thingName} private.key', 'w') as outfile:
			outfile.write(PrivateKey)
	with open(f'certs/{thingName} cert.pem', 'w') as outfile:
			outfile.write(certificatePem)

	response = thingClient.attach_policy(
			policyName = defaultPolicyName,
			target = certificateArn
	)
	response = thingClient.attach_thing_principal(
			thingName = thingName,
			principal = certificateArn
	)


def add_thing_to_group():
	global thingClient
	response = thingClient.add_thing_to_thing_group(
        thingGroupName='Many_iot_thing_group',
        thingGroupArn='arn:aws:iot:us-east-2:390403862750:thinggroup/Many_iot_thing_group',
        thingName=thingName,
        thingArn=thingArn,
        overrideDynamicGroups=False
        )

thingClient = boto3.client('iot',aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,)
# createThing()

for n in range(500):
	
    thingArn = ''
    thingId = ''
    thingName = 'Lab4_IoT_Thing-'+str(n)
    defaultPolicyName = 'My_Iot_grp_Policy'
    createThing()




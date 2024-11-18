import sys

message = "Hello, %s!" % sys.argv[1]
message += " Greetings from your first Greengrass component."
# Print the message to stdout, which Greengrass saves in a log file.
print(message)
print ('gwijaya ending message')

#location
#s3://greengrass-component-artifacts-us-east-2-796973478303/artifacts/com.example.HelloWorld/1.0.0/hello_world.py
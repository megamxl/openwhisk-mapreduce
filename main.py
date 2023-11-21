import requests
import base64
from minio import Minio
import io
import json
import http.client
import time
import threading
import urllib3

urllib3.disable_warnings()


with open('config.json') as config:
    cofing_contnet = config.read()

parsed_config = json.loads(cofing_contnet)

client = Minio(
    endpoint=parsed_config['minio-enpoint'],
    access_key=parsed_config['minio-access_key'],
    secret_key=parsed_config['minio-secret_key'],
    secure=False
)

auth_key = "NWI3MzhiNjYtMWRhYS00ZWJhLWJlZTAtMjM3ZWEyZGU0Njk4OjdoejhVakJMR1o0Wjc3NFNFbHZlOE1LVndTVVdyMmw2bzd5aHR0akJIOFk1ODhzUU9XdHA4cDNqT0ZIdUFHQXc="
runtime = "python:3"

def deploy_a_function(auth_key, runtime, zip_file_path, url, functionName, mainFunction ):

    # Prepare the headers
    headers = {
        "Authorization": f"Basic {auth_key}",
        "Content-Type": "application/json",
    }

    with open(zip_file_path, "rb") as zip_file:
        code_content = base64.b64encode(zip_file.read()).decode("utf-8")

    # Prepare the payload
    payload = {
        "namespace":"_",
        "name": functionName,
        "exec": {
            "kind": runtime,
            "code": code_content,
            "main": mainFunction
        },
        "annotations": [
            {
               "key": "limits.maxInvocations",
                "value": 100
            }
        ],
        "limits": {
            "concurrency": 1
        }
    }

    response = requests.put(url, headers=headers, json=payload, verify=False)

    # Check the response
    if response.status_code == 200:
        print(f"Function {functionName} deployed successfully!")
    else:
        print(f"Failed to deploy function. Status code: {response.status_code}")
        print("Error message:", response.text)


def invoke_a_function(auth_key, url, body ):

    # Prepare the headers
    headers = {
        "Authorization": f"Basic {auth_key}",
        "Content-Type": "application/json",
    }

    response = requests.post(url, headers=headers, json=body, verify=False)

    # Check the response
    if response.status_code == 200:
        print(response.content)
    else:
        print(f"Failed to deploy function. Status code: {response.status_code}")
        print("Error message:", response.text)

input_bucket = parsed_config['bucket-prefix'] + "-input"

client.make_bucket(bucket_name=input_bucket)

usedkeys = []

batch_size = parsed_config['input-bath-size']

print("start with data spliting")
with open("exampleData/data.csv") as fl:
    key = 0
    lines = []
    for index, line in enumerate(fl):
        if index > 0 and index % batch_size == 0:
            # put in object storage
            client.put_object(input_bucket, str(key),  io.BytesIO(json.dumps(lines).encode('utf-8')), content_type= "application/json", length=-1, part_size=10*1024*1024)
            lines.clear()
            usedkeys.append(key)
            key = key +1
            #TODO ROEMOVE
            if key == 11:
                break 
        lines.append(line)

mapper_function_name = parsed_config['bucket-prefix'] + "-mapper"

zip_file_path = "mapOp/minotets.zip"
functionName = parsed_config['mapFunction']
url = f"https://192.168.178.220:443/api/v1/namespaces/_/actions/{functionName}?overwrite=false"

deploy_a_function(auth_key=auth_key, runtime=runtime, zip_file_path=zip_file_path, url=url, functionName=functionName , mainFunction=parsed_config['mapFunction-main'])
print(f"depolyed mapper {functionName}")

intermidated_buket = parsed_config['bucket-prefix'] + "-intermidated"

client.make_bucket(bucket_name=intermidated_buket)

url = f"https://192.168.178.220:443/api/v1/namespaces/_/actions/{functionName}?blocking=true&result=true"

threads = []

for key in usedkeys:
    data = {
        "bucketName": input_bucket,
        "key": str(key),
        "outputBucket": intermidated_buket
    }
    #invoke_a_function(auth_key=auth_key, url=url, body=body)
    thread = threading.Thread(target=invoke_a_function, args=(auth_key, url, data,))
    threads.append(thread)

print("starting all mapper calls")
for thread in threads:
    thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()

print("All HTTP calls completed.")   
 
threads.clear()

paths = client.list_objects(intermidated_buket, prefix="key", recursive=True)

intermediatekeys = []

reducer_function_name = parsed_config['bucket-prefix'] + "-reducer"

url = f"https://192.168.178.220:443/api/v1/namespaces/_/actions/{parsed_config['reduceFunction']}?overwrite=false"


deploy_a_function(auth_key=auth_key, runtime=runtime, zip_file_path="redOp/reducertest.zip", url=url, functionName=parsed_config['reduceFunction'] , mainFunction=parsed_config['reduceFunction-main'])

output_bucket = parsed_config['bucket-prefix'] + "-output"

client.make_bucket(bucket_name=output_bucket)

url = f"https://192.168.178.220:443/api/v1/namespaces/_/actions/{parsed_config['reduceFunction']}?blocking=true&result=true"


print("getting all intermediate keys")
for object in paths:
    intermediatekeys.append(object.object_name.split("/")[1])

for idx, key_1 in enumerate(intermediatekeys):
    data = {
        "bucketName": intermidated_buket,
        "key": str(key_1),
        "outputBucket": output_bucket
    }
    if idx > 0 and idx % 10 == 0:
        for thread in threads:
            thread.start()

    # Wait for all threads to finish
        for thread in threads:
            thread.join()
        time.sleep(65)
        threads.clear()
    
    thread = threading.Thread(target=invoke_a_function, args=(auth_key,url, data,))
    threads.append(thread)

print("All HTTP calls completed. reduce")
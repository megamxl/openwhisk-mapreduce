import requests
import base64
from minio import Minio
import io
import json
import time
import threading
import urllib3

urllib3.disable_warnings()


with open('dockerConfig.json') as config:
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

def deploy_a_function(auth_key, runtime, zip_file_path, url, functionName, mainFunction, docker ):

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
                "value": 100000
            }
        ],
        "limits": {
            "concurrency": 5,
            "timeout" : 250000
        }
    }


    if docker:
        payload = {
        "namespace":"_",
        "name": functionName,
        "exec": {
            "kind": "blackbox",
            "image": mainFunction
        },       
           "annotations": [
            {
               "key": "limits.maxInvocations",
                "value": 100000
            }
        ],
        "limits": {
            "concurrency": 5,
            "timeout" : 250000
        }
    }


    response = requests.put(url, headers=headers, json=payload, verify=False)

    # Check the response
    if response.status_code == 200:
        print(f"Function {functionName} deployed successfully!")
    elif response.status_code ==409:
        print("Abort function didnt deployed")
        raise Exception("Cant do shit", response.text)
    else:
        print(f"Failed to deploy function. Status code: {response.status_code}")
        print("Error message:", response.text)
    print("Now sleeping for openwhisk")
    #time.sleep(60*5)


def invoke_a_function(auth_key, url, body ):

    # Prepare the headers
    headers = {
        "Authorization": f"Basic {auth_key}",
        "Content-Type": "application/json",
    }

    response = requests.post(url, headers=headers, json=body, verify=False)

    if response.status_code != 200 and  response.status_code != 202:
        failed_req.append([auth_key,url,body])
        print("Error message:", response.text)
    elif response.status_code == 202:  
        activation_ids.append(json.loads(response.text)['activationId'])
        if parsed_config['debug']: 
            print(f"$ recieved one activatioin id{response.text}")
    else:
        if parsed_config['debug']: 
            print(response.text)

def reDoingRequests(req):
    localtread = []
    for idx, key_1 in enumerate(req):
        if idx > 0 and idx % 20 == 0:
            for thread in localtread:
                thread.start()

            # Wait for all threads to finish
            for thread in localtread:
                thread.join()
            
            localtread.clear()
        
        thread = threading.Thread(target=invoke_a_function, args=(key_1[0], key_1[1], key_1[2]))
        localtread.append(thread)


#TODO Activation id handler 
def handle_activationID(req):
    for act_id in req:
        continue

def HandleRequests(auth_key, invoke_a_function, reDoingRequests, handle_activationID, input_bucket, usedkeys, failed_req, activation_ids, url, output_buket, batchNumber):
    threads = []

    inv_time = time.time()
    inv_counter = 0
    print(f"Doing {len(usedkeys)} mapper calls ")
    for idx, key_1 in enumerate(usedkeys):
        data ={
        "bucketName": input_bucket,
        "key": str(key_1),
        "outputBucket": output_buket
    }
        if idx > 0 and idx % batchNumber == 0:
            inv_counter = inv_counter + 50
            if(inv_counter > 3600):
                if(time.time() - inv_time < 61):
                    print(f"now sleeping {time.time() - inv_time} seconds")
                    time.sleep(time.time() - inv_time)
                    inv_time = time.time()
                    inv_counter = 0
                else:
                    inv_counter= 0
                    inv_time = time.time()

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()
        
            threads.clear()
            print("")
    
        thread = threading.Thread(target=invoke_a_function, args=(auth_key, url, data,))
        threads.append(thread)

    for thread in threads:
                thread.start()

# Wait for all threads to finish
    for thread in threads:
        thread.join()

    threads.clear()

    print(f"there were {len(failed_req)} failed request doing them again")

    if len(failed_req) != 0:
        reDoingRequests(failed_req)
    
    failed_req.clear()

    print(f"there were {len(activation_ids)} request which arent returnt properly")

    if len(activation_ids) != 0:
        handle_activationID(failed_req)
    
    failed_req.clear()

###
# 
#  First Step take the file and create chunks and put them into Minio
#
###

input_bucket = parsed_config['bucket-prefix'] + "-input"

print("making Input bucket")
client.make_bucket(bucket_name=input_bucket)

usedkeys = []
failed_req = []
activation_ids = []

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
            if key == 10:
                break 
        lines.append(line)


print("done splitiing data")
mapper_function_name = parsed_config['bucket-prefix'] + "-mapper"

zip_file_path = "mapOp/m.zip"
functionName = parsed_config['mapFunction']
url = f"https://192.168.178.220:443/api/v1/namespaces/_/actions/{functionName}?overwrite=false"

star_time = time.time()

deploy_a_function(auth_key=auth_key, runtime=runtime, zip_file_path=zip_file_path, url=url, functionName=functionName , mainFunction=parsed_config['mapFunction-main'], docker=parsed_config['docker'])
print(f"depolyed mapper {functionName}")

intermidated_buket = parsed_config['bucket-prefix'] + "-intermidated"

client.make_bucket(bucket_name=intermidated_buket)

url = f"https://192.168.178.220:443/api/v1/namespaces/_/actions/{functionName}?blocking=true&result=true"

if parsed_config['debug']:
    print("Metadata : start mapper calcl here")

HandleRequests(auth_key, invoke_a_function, reDoingRequests, handle_activationID, input_bucket, usedkeys, failed_req, activation_ids, url, intermidated_buket, 10)

if parsed_config['debug']:
    print("Metadata : sopt mapper calcl here")

print("All Mapper calls completed.")   

paths = client.list_objects(intermidated_buket, prefix="key", recursive=True)

intermediatekeys = []

reducer_function_name = parsed_config['bucket-prefix'] + "-reducer"

url = f"https://192.168.178.220:443/api/v1/namespaces/_/actions/{parsed_config['reduceFunction']}?overwrite=false"

print("Deploying Reducer function")
deploy_a_function(auth_key=auth_key, runtime=runtime, zip_file_path="redOp/reducertest.zip", url=url, functionName=parsed_config['reduceFunction'] , mainFunction=parsed_config['reduceFunction-main'],  docker=parsed_config['docker'])

output_bucket = parsed_config['bucket-prefix'] + "-output"

print("Create Output Bucket")
client.make_bucket(bucket_name=output_bucket)

url = f"https://192.168.178.220:443/api/v1/namespaces/_/actions/{parsed_config['reduceFunction']}?blocking=true&result=true"


print("getting all intermediate keys")
for object in paths:
    intermediatekeys.append(object.object_name.split("/")[1])

HandleRequests(auth_key, invoke_a_function, reDoingRequests, handle_activationID, intermidated_buket, intermediatekeys,failed_req, activation_ids, url,output_bucket, 20)

end_time = time.time()

sec_between = end_time -star_time

print("All Done Here are your statistics nows")
print("")
print("")
print(f"The time elapesd between the first fuction deploy until now are {sec_between}" )
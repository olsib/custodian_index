import boto3
import re
import requests
import gzip
import urllib
import io
import cStringIO
from requests_aws4auth import AWS4Auth

region = 'eu-west-1' # e.g. us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = 'Put your endpoint in here' # the Amazon ES domain, including https://
index = 'lambda-s3-index'
type = 'lambda-type'
url = host + '/' + index + '/' + type

headers = { "Content-Type": "application/json" }

s3 = boto3.client('s3')

# Regular expressions used to parse custodian log lines
time_pattern = re.compile('(\d+\-\d+\-\d+\s\d+:\d+:\d+)')
policy_pattern = re.compile('(policy:.+?\s)')
resource_pattern = re.compile('(resource:.+?\s)')
region_pattern = re.compile('(region:.+?\s)')
count_pattern = re.compile('(count:.+?\s)')

# Lambda execution starts here
def handler(event, context):
    print(event)
    for record in event['Records']:

        # Get the bucket name and key for the new file
        bucket = record['s3']['bucket']['name']
        print bucket
        #key = record['s3']['object']['key']
        key = urllib.unquote_plus(record['s3']['object']['key'].encode('utf8'))
        print key
        # Grab account id 12 digits from path
        account_pattern = re.compile('(\d{12})')
        account_id = account_pattern.search(key).group(1)
        print account_id
        # Get, read, and split the file into lines
        obj = s3.get_object(Bucket=bucket, Key=key)
        print obj
        bytestream = io.BytesIO(obj['Body'].read())
        print bytestream
        gzipfile = gzip.GzipFile(mode='rt',fileobj=bytestream).read()
        print gzipfile
        lines = gzipfile.splitlines()
        print lines
        
        # Match the regular expressions to each line and index the JSON
        for line in lines:
            try:
                print line
                timestamp = time_pattern.search(line).group(1)
                print timestamp
                policy = policy_pattern.search(line).group(1)
                policy_trimmed = policy.rpartition(':')[2].strip()
                print policy_trimmed
                resource = resource_pattern.search(line).group(1)
                resource_trimmed = resource.rpartition(':')[2].strip()
                print resource_trimmed
                region = region_pattern.search(line).group(1)
                region_trimmed = region.rpartition(':')[2].strip()
                print region_trimmed
                count = count_pattern.search(line).group(1)
                count_trimmed = count.rpartition(':')[2].strip()
                print count_trimmed
                document = { "timestamp": timestamp, "policy": policy_trimmed, "resource": resource_trimmed, "account_id": account_id, "region": region_trimmed, "count": count_trimmed }
                print(document)
                r = requests.post(url, auth=awsauth, json=document, headers=headers)
                print(r.status_code)
                pass
            except:
                continue

import json
import boto3
import urllib3
from urllib.parse import unquote_plus
import os

# Initialize clients
s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')
http = urllib3.PoolManager()

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))

    # 1. Get the bucket name and file key from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    print(f"Processing file: {key} from bucket: {bucket}")

    try:
        # 2. Call Rekognition to detect labels
        rekog_response = rekognition.detect_labels(
            Image={'S3Object': {'Bucket': bucket, 'Name': key}},
            MaxLabels=10,
            MinConfidence=75
        )
        
        # Extract labels into a simple list
        labels = [label['Name'] for label in rekog_response['Labels']]
        print(f"Rekognition found: {labels}")

        # 3. Get S3 Metadata (for custom labels x-amz-meta-customLabels)
        metadata = s3.head_object(Bucket=bucket, Key=key)
        custom_labels = metadata.get('Metadata', {}).get('customlabels', '')
        
        if custom_labels:
            custom_list = [l.strip() for l in custom_labels.split(',')]
            labels.extend(custom_list)
            print(f"Added custom labels: {custom_list}")

        # 4. Prepare the JSON object for OpenSearch
        document = {
            "objectKey": key,
            "bucket": bucket,
            "createdTimestamp": metadata['LastModified'].strftime("%Y-%m-%dT%H:%M:%S"),
            "labels": labels
        }

        # 5. Send to OpenSearch
        # Retrieve OpenSearch credentials and host from Environment Variables
        os_host = os.environ['OS_HOST'] # We will set this in a moment
        os_user = os.environ['OS_USER']
        os_pass = os.environ['OS_PASS']
        
        # OpenSearch URL (Ensure your OS_HOST doesn't have https:// prefix, or adjust code)
        url = f"https://{os_host}/photos/_doc"
        
        # Make the HTTP POST request using Basic Auth
        headers = urllib3.util.make_headers(basic_auth=f"{os_user}:{os_pass}")
        headers['Content-Type'] = 'application/json'
        
        response = http.request(
            'POST',
            url,
            body=json.dumps(document),
            headers=headers
        )
        
        print(f"OpenSearch response status: {response.status}")
        print(response.data.decode('utf-8'))
        
        return {
            'statusCode': 200,
            'body': json.dumps('Photo indexed successfully!')
        }

    except Exception as e:
        print(f"Error processing object {key} from bucket {bucket}. Event: {json.dumps(event)}")
        print(str(e))
        raise e

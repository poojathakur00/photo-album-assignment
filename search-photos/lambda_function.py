import json
import boto3
import urllib3
import os

# Initialize clients
# Force the client to look in N. Virginia (us-east-1) for the Bot
lex_client = boto3.client('lexv2-runtime', region_name='us-east-1')
http = urllib3.PoolManager()

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))
    
    # 1. Extract the user's query text
    user_query = "everything" # Default fallback
    if event.get('queryStringParameters') and event.get('queryStringParameters').get('q'):
        user_query = event['queryStringParameters']['q']
    
    print(f"User query: {user_query}")

    # 2. Call Lex to "Disambiguate" (Extract keywords)
    bot_id = os.environ['BOT_ID']
    bot_alias_id = os.environ['BOT_ALIAS_ID']
    
    try:
        lex_response = lex_client.recognize_text(
            botId=bot_id,
            botAliasId=bot_alias_id,
            localeId='en_US',
            sessionId='test-session-user',
            text=user_query
        )
        print("Lex Response:", json.dumps(lex_response))
        
        # 3. Extract Slots (Keywords) from Lex
        keywords = []
        slots = lex_response.get('sessionState', {}).get('intent', {}).get('slots', {})
        
        if slots:
            for slot_name, slot_data in slots.items():
                if slot_data and slot_data.get('value'):
                    keywords.append(slot_data['value']['interpretedValue'])
        
        print(f"Keywords extracted: {keywords}")
        
    except Exception as e:
        print("Error calling Lex:", str(e))
        # Fallback: if Lex fails, just search for the raw query
        keywords = [user_query]

    if not keywords:
        keywords = [user_query]

    # 4. Search OpenSearch
    os_host = os.environ['OS_HOST']
    os_user = os.environ['OS_USER']
    os_pass = os.environ['OS_PASS']
    url = f"https://{os_host}/photos/_search"
    
    # Construct the search query
    should_clauses = []
    for k in keywords:
        should_clauses.append({"match": {"labels": k}})
        
    query = {
        "size": 5,
        "query": {
            "bool": {
                "should": should_clauses
            }
        }
    }
    
    # Make the HTTP request
    headers = urllib3.util.make_headers(basic_auth=f"{os_user}:{os_pass}")
    headers['Content-Type'] = 'application/json'
    
    response = http.request('GET', url, body=json.dumps(query), headers=headers)
    
    # 5. Format results for Frontend
    os_data = json.loads(response.data.decode('utf-8'))
    hits = os_data.get('hits', {}).get('hits', [])
    
    results = []
    for hit in hits:
        source = hit['_source']
        results.append({
            'url': f"https://{source['bucket']}.s3.amazonaws.com/{source['objectKey']}",
            'labels': source['labels']
        })
        
    # RETURN RESPONSE WITH CORS HEADERS
    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,GET"
        },
        'body': json.dumps(results)
    }

import os
import json
from pymongo import MongoClient
from bson import ObjectId
import boto3 

ATLAS_CONNECTION_STRING = os.environ['ATLAS_CONN_STR']
DB_NAME = os.environ['DB_NAME']
COLLECTION = os.environ['COLLECTION']
SEARCH_INDEX = os.environ['SEARCH_INDEX']
VECTOR_FIELD = os.environ['VECTOR_FIELD']
client = MongoClient(ATLAS_CONNECTION_STRING)
bedrock_runtime = boto3.client('bedrock-runtime', region_name='us-east-1')

def lambda_handler(event, context):
    agent = event['agent']
    actionGroup = event['actionGroup']
    function = event['function']
    parameters = event.get('parameters', [])

    
    def vector_search(query, limiter=10):
        agg_pipeline = [
            {
                "$vectorSearch": {
                    "index": SEARCH_INDEX,
                    "path": VECTOR_FIELD,
                    "queryVector": query_vector,
                    "numCandidates": 100,
                    "limit": limiter
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "title": 1
                }
            } 
        ]

        return list(client[DB_NAME][COLLECTION].aggregate(agg_pipeline))


    param_dict = {param['name'].lower(): (int(param['value']) if param['type'] == "number" else param['value']) for param in parameters}
    query, limiter = param_dict.get("query"), param_dict.get("limiter", 5)
    response = bedrock_runtime.invoke_model(
        modelId="amazon.titan-embed-text-v1",
        body=json.dumps({"inputText": query}),
        contentType="application/json",
        accept="application/json"
    )

    query_vector = json.loads(response['body'].read()).get("embedding")

    if query is not None and query_vector:
        try:
            result = vector_search(query, limiter)
            result_txt = "Vector Search Results: \n{}".format(result)
        except ValueError:
            result_txt = "Error: Issue with query or vector parameter type"
    else:
        result_txt = "Missing query or vector"

    # Execute your business logic here. For more information, refer to: https://docs.aws.amazon.com/bedrock/latest/userguide/agents-lambda.html
    responseBody =  {
        "TEXT": {
            "body": result_txt
        }
    }

    action_response = {
        'actionGroup': actionGroup,
        'function': function,
        'functionResponse': {
            'responseBody': responseBody
        }

    }

    dummy_function_response = {'response': action_response, 'messageVersion': event['messageVersion']}
    print("Response: {}".format(dummy_function_response))

    return dummy_function_response

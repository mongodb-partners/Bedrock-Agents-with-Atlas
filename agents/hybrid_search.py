import os
import json
from pymongo import MongoClient
from bson import ObjectId
import boto3 

ATLAS_CONNECTION_STRING = os.environ.get('ATLAS_CONN_STR', 'mongodb+srv://anujpanchal:anujpanchal@aws-gp.ls3c2.mongodb.net')
DB_NAME = os.environ.get('DB_NAME', 'sample_mflix')
COLLECTION = os.environ.get('COLLECTION', 'embedded_movies')
SEARCH_INDEX = os.environ.get('SEARCH_INDEX', 'default')
VECTOR_INDEX = os.environ.get('VECTOR_INDEX', 'vector_index')  
VECTOR_FIELD = os.environ.get('VECTOR_FIELD', 'plot_embedding')
client = MongoClient(ATLAS_CONNECTION_STRING)
bedrock_runtime = boto3.client('bedrock-runtime', region_name='us-east-1')

def lambda_handler(event, context):
    agent = event['agent']
    actionGroup = event['actionGroup']
    function = event['function']
    parameters = event.get('parameters', [])

    # Function for hybrid search (text + vector search)
    def hybrid_search(query, limiter=10):

        hybrid_stage = pipeline = [
            {
                "$vectorSearch": {
                    "index": VECTOR_INDEX,
                    "path": VECTOR_FIELD,
                    "queryVector": query_vector,
                    "numCandidates": 100,
                    "limit": limiter
                }
            },
            {"$group": {"_id": None, "docs": {"$push": "$$ROOT"}}},
            {"$unwind": {"path": "$docs", "includeArrayIndex": "rank"}},
            {"$addFields": {
                "vs_score": {"$multiply": [0.5, {"$divide": [1.0, {"$add": ["$rank", 60]}]}]}
            }},
            {"$project": {"vs_score": 1, "_id": "$docs._id", "title": "$docs.title"}},
            {"$unionWith": {
                "coll": COLLECTION,
                "pipeline": [
                    {"$search": {"index": SEARCH_INDEX, "phrase": {"query": query, "path": "title"}}},
                    {"$limit": limiter},
                    {"$group": {"_id": None, "docs": {"$push": "$$ROOT"}}},
                    {"$unwind": {"path": "$docs", "includeArrayIndex": "rank"}},
                    {"$addFields": {"fts_score": {"$multiply": [0.5, {"$divide": [1.0, {"$add": ["$rank", 60]}]}]}}},
                    {"$project": {"fts_score": 1, "_id": "$docs._id", "title": "$docs.title"}}
                ]
            }},
            {"$group": {"_id": "$_id", "title": {"$first": "$title"}, "vs_score": {"$max": "$vs_score"}, "fts_score": {"$max": "$fts_score"}}},
            {"$project": {"_id": 1, "title": 1, "vs_score": {"$ifNull": ["$vs_score", 0]}, "fts_score": {"$ifNull": ["$fts_score", 0]}}},
            {"$project": {"score": {"$add": ["$fts_score", "$vs_score"]}, "_id": 1, "title": 1, "vs_score": 1, "fts_score": 1}},
            {"$sort": {"score": -1}},
            {"$limit": limiter}
        ]
        return list(client[DB_NAME][COLLECTION].aggregate(hybrid_stage))

    # Extracting values from the params
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
            result = hybrid_search(query, limiter)
            result_txt = "Hybrid Search Results: \n{}".format(result)
        except ValueError:
            result_txt = "Error: Issue with query or vector parameter type"
    else:
        result_txt = "Missing query or vector"

    responseBody = {
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

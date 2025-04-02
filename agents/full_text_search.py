import os
import json
from pymongo import MongoClient
from bson import ObjectId

ATLAS_CONNECTION_STRING = os.environ['ATLAS_CONN_STR']
DB_NAME = os.environ['DB_NAME']
COLLECTION = os.environ['COLLECTION']
SEARCH_INDEX = os.environ['SEARCH_INDEX']
client = MongoClient(ATLAS_CONNECTION_STRING)

def lambda_handler(event, context):
    agent = event['agent']
    actionGroup = event['actionGroup']
    function = event['function']
    parameters = event.get('parameters', [])

    # Function to search restaurants based on the keyword
    def search_restaurants(keyword, limiter=5):
        search_stage = [
            {
                "$search":
                {
                    "index": SEARCH_INDEX,
                    "text": {
                        "query": keyword,
                        "path": {
                            "wildcard": "*"
                        }
                    }
                }
            },
            {
                "$project":
                {
                    "_id": 0,
                    "name": 1,
                    "address": {
                        "$concat": [
                            "Building - ",
                            "$address.building",
                            ", ",
                            "$address.street",
                            ", ", 
                            "$borough",
                            " - ",
                            "$address.zipcode"
                        ]
                    },
                    "score": { "$meta": "searchScore" }
                }
            },
            {
                "$sort": {
                    "score": -1
                }
            },
            {
                "$limit": limiter
            }
        ]
        return list(client[DB_NAME][COLLECTION].aggregate(search_stage))

    # Extracting values from the params
    param_dict = {param['name'].lower(): (int(param['value']) if param['type'] == "number" else param['value']) for param in parameters}

    # Check the function name and execute the corresponding action
    if function == "search_restaurants":
        keyword, limiter = param_dict.get("keyword"), param_dict.get("limiter", 5)
        if keyword is not None:
            try:
                result = search_restaurants(keyword, limiter)
                result_txt = "List of Restaurants: \n{}".format(result)
            except ValueError:
                result_txt = "Error: Some issue with the keyword parameter type"
        else:
            result_txt = "Missing keyword"

        responseBody = {
            "TEXT": {
                "body": result_txt
            }
        }
    else:
        responseBody =  {
            "TEXT": {
                "body": "The function {} was called successfully!".format(function)
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

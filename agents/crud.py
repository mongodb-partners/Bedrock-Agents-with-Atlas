import os
import json
from pymongo import MongoClient
from bson import ObjectId

ATLAS_CONNECTION_STRING = os.environ['ATLAS_CONN_STR']
DB_NAME = os.environ['DB_NAME']
COLLECTION = os.environ['COLLECTION']
client = MongoClient(ATLAS_CONNECTION_STRING)

def lambda_handler(event, context):
    agent = event['agent']
    actionGroup = event['actionGroup']
    function = event['function']
    parameters = event.get('parameters', [])

    def insert_one(name, age):
        return client[DB_NAME][COLLECTION].insert_one({"name": name, "age": age})

    def find_one(name):
        return client[DB_NAME][COLLECTION].find_one({"name": name}, {"_id": 0})

    def delete_one(name):
        return client[DB_NAME][COLLECTION].delete_one({"name": name})
    
    def update_one(name, age):
        return client[DB_NAME][COLLECTION].update_one({"name": name}, {"$set": {"name": name, "age": age}})

    # Extracting values from the params
    param_dict = {param['name'].lower(): (int(param['value']) if param['type'] == "number" else param['value']) for param in parameters}

    # Check the function name and execute the corresponding action
    if function == "insert_one":
        name, age = param_dict.get("name"), param_dict.get("age")
        if name is not None and age is not None:
            try:
                result = insert_one(name=name, age=age)
                result_txt = "Record added successfully"
            except ValueError:
                result_txt = "Error: Some issue with the name/age parameter type"
        else:
            result_txt = "Missing param"

        responseBody = {
            "TEXT": {
                "body": result_txt
            }
        }
    elif function == "find_one":
        name = param_dict.get("name")
        if name is not None:
            result = find_one(name=name)
            if result is not None:
                result_txt = "Record found: {}".format(result)
            else:
                result_txt = "No record with this name found"
        else:
            result_txt = "Missing param: Name"
        
        responseBody = {
            "TEXT": {
                "body": result_txt
            }
        }
    elif function == "delete_one":
        name = param_dict.get("name")
        if name is not None:
            result = delete_one(name=name)
            if result is not None:
                result_txt = "Record deleted successfully"
            else:
                result_txt = "No record with this name found"
        else:
            result_txt = "Missing param: Name"
        
        responseBody = {
            "TEXT": {
                "body": result_txt
            }
        }
    elif function == "update_one":
        name, age = param_dict.get("name"), param_dict.get("age")
        if name is not None and age is not None:
            try:
                record_exists = client[DB_NAME][COLLECTION].find_one({"name": name})
                if record_exists is not None:
                    result = update_one(name=name, age=age)
                    result_txt = "Record updated successfully"
                else:
                    result_txt = "Record with name: {} not found".format(name)
            except ValueError:
                result_txt = "Error: Some issue with the name/age parameter type"
        else:
            result_txt = "Missing param"

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

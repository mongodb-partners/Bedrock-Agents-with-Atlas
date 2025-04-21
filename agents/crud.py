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
    print("Action Group: ", actionGroup)
    function = event['function']
    parameters = event.get('parameters', [])
    print("Parameters: ", parameters)

    def insert_one(params):
        return client[DB_NAME][COLLECTION].insert_one(params)
    
    def insert_many(params):
        return client[DB_NAME][COLLECTION].insert_many(params)

    def find_one(params):
        return client[DB_NAME][COLLECTION].find_one(params)

    def find_many(filter_obj, projection_obj={}):
        if len(projection_obj) > 0:
            return list(client[DB_NAME][COLLECTION].find(filter_obj, projection_obj))
        else:
            return list(client[DB_NAME][COLLECTION].find(filter_obj))

    def delete_one(params):
        return client[DB_NAME][COLLECTION].delete_one(params)
    
    def delete_many(params):
        return client[DB_NAME][COLLECTION].delete_many(params)
    
    def update_one(filter_obj, update_obj):
        return client[DB_NAME][COLLECTION].update_one(filter_obj, update_obj)
    
    def update_many(filter_obj, update_obj):
        return client[DB_NAME][COLLECTION].update_many(filter_obj, update_obj)

    # Extracting values from the params
    param_dict = {param['name'].lower(): (int(param['value']) if param['type'] == "number" else param['value']) for param in parameters}
    json_obj = json.loads(param_dict['json_obj'])
    # Check the function name and execute the corresponding action
    if function == "insert_one":
        if len(json_obj) > 0:
            try:
                result = insert_one(json_obj)
                result_txt = "Record added successfully"
            except ValueError:
                result_txt = "Error: Some issue with the parameter type"
        else:
            result_txt = "Missing param"

        responseBody = {
            "TEXT": {
                "body": result_txt
            }
        }
    elif function == "find_one":
        if json_obj is not None:
            result = find_one(json_obj)
            if result is not None:
                result_txt = "Record found: {}".format(result)
            else:
                result_txt = "No record found"
        else:
            result_txt = "Missing param"
        
        responseBody = {
            "TEXT": {
                "body": result_txt
            }
        }
    elif function == "delete_one":
        if json_obj is not None:
            result = delete_one(json_obj)
            if result is not None:
                result_txt = "Record deleted successfully"
            else:
                result_txt = "No record found"
        else:
            result_txt = "Missing param"
        
        responseBody = {
            "TEXT": {
                "body": result_txt
            }
        }
    elif function == "update_one":
        if 'filter' in json_obj and 'update' in json_obj:
            try:
                record_exists = find_one(json_obj['filter'])
                if record_exists is not None:
                    result = update_one(json_obj['filter'], json_obj['update'])
                    result_txt = "Record updated successfully"
                else:
                    result_txt = "Record not found"
            except ValueError:
                result_txt = "Error: Some issue with the parameter type"
        else:
            result_txt = "Missing param"

        responseBody = {
            "TEXT": {
                "body": result_txt
            }
        }
    if function == "insert_many":
        if json_obj is not None:
            try:
                result = insert_many(json_obj)
                result_txt = "Records added successfully"
            except ValueError:
                result_txt = "Error: Some issue with the parameter type"
        else:
            result_txt = "Missing param"

        responseBody = {
            "TEXT": {
                "body": result_txt
            }
        }
    elif function == "find_many":
        projection_obj = json_obj['projection'] if 'projection' in json_obj else {}
        if 'filter' in json_obj:
            result = find_many(json_obj['filter'], projection_obj)
            if result is not None:
                result_txt = "Record found: {}".format(result)
            else:
                result_txt = "No records found"
        else:
            result_txt = "Missing param"
        
        responseBody = {
            "TEXT": {
                "body": result_txt
            }
        }
    elif function == "delete_many":
        if json_obj is not None:
            result = delete_many(json_obj)
            if result is not None:
                result_txt = "Record deleted successfully"
            else:
                result_txt = "No records found"
        else:
            result_txt = "Missing param"
        
        responseBody = {
            "TEXT": {
                "body": result_txt
            }
        }
    elif function == "update_many":
        if 'filter' in json_obj and 'update' in json_obj:
            try:
                result = update_many(json_obj['filter'], json_obj['update'])
                result_txt = "Records updated successfully"
            except ValueError:
                result_txt = "Error: Some issue with the parameter type"
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

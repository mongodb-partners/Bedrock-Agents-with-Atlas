import time
import logging

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.event_handler import BedrockAgentResolver
from aws_lambda_powertools.utilities.typing import LambdaContext
from typing_extensions import Annotated
from aws_lambda_powertools.event_handler.openapi.params import Body, Query
from pymongo import MongoClient
import os
from langchain_aws.embeddings import BedrockEmbeddings
import boto3
from botocore.exceptions import ClientError

tracer = Tracer()
logger = Logger()
app = BedrockAgentResolver()


@app.get("/current_time", description="Gets the current time in seconds")  
@tracer.capture_method
def current_time() -> int:
    logger.info("Getting current time in seconds")
    return int(time.time())

@app.get("/current_month", description="Gets the current month")  
@tracer.capture_method
def current_month() -> str:
    logger.info("Getting current month")
    return time.strftime("%B")

@app.get("/get_place_by_country", description="Retrieve places by country name")
@tracer.capture_method
def place_lookup_by_country(query_str: Annotated[str, Query(description="The country name")]
                                    ) -> Annotated[str, Body(description="Place Names")]:
    logger.info(f"Looking up places by country: {query_str}")
    client = get_mongo_client()
    # get database and collection
    collection = get_travel_collection(client)
    res = ""
    res = collection.aggregate(
        [
            {"$match": {"Country": {"$regex": query_str, "$options": "i"}}},
            {"$project": {"Place Name": 1}},
        ]
    )
    places = []
    for place in res:
        places.append(place["Place Name"])
    logger.info(f"Found {len(places)} places in country: {query_str}")
    return str(places)

def get_travel_collection(client):
    logger.info("Getting travel collection from MongoDB")
    db = client['Integration']
    collection = db['test_csv_load']
    return collection

@app.get("/get_place_by_name", description="Retrieve place information by place name")
@tracer.capture_method
def place_lookup_by_name(query_str: Annotated[str, Query(description="The place name")]
                                 ) -> Annotated[str, Body(description="Place Details")]:
    logger.info(f"Looking up place by name: {query_str}")
    client = get_mongo_client()
    collection = get_travel_collection(client)
    res = ""
    filter = {
        "$or": [
            {"Place Name": {"$regex": query_str, "$options": "i"}},
            {"Country": {"$regex": query_str, "$options": "i"}},
        ]
    }
    project = {"_id": 0}

    res = collection.find_one(filter=filter, projection=project)
    logger.info(f"Found place details for: {query_str}")
    return str(res)

@app.get("/get_place_best_time", description="Retrieve place's best time to visit")
@tracer.capture_method
def place_best_time_lookup(query_str: Annotated[str, Query(description="The place name")]
                                               ) -> Annotated[str, Body(description="Place's best time to visit")]:
    logger.info(f"Looking up best time to visit for place: {query_str}")
    client = get_mongo_client()
    collection = get_travel_collection(client)
    res = ""
    filter = {
        "$or": [
            {"Place Name": {"$regex": query_str, "$options": "i"}},
            {"Country": {"$regex": query_str, "$options": "i"}},
        ]
    }
    project = {"Best Time To Visit": 1, "_id": 0}

    res = collection.find_one(filter=filter, projection=project)
    logger.info(f"Found best time to visit for: {query_str}")
    return str(res)

# Setup bedrock
def setup_bedrock():
    """Initialize the Bedrock runtime."""
    logger.info("Setting up Bedrock runtime client")
    return boto3.client(
        service_name="bedrock-runtime",
        region_name="us-east-1",
    )


@app.get("/get_place_semantically", description="Retrieve place information by place features")
@tracer.capture_method
def mongodb_search(query: Annotated[str,Query(description="place features")]) -> Annotated[str, Body(description="Places and their details")]:
    logger.info(f"Performing semantic search for place features: {query}")
    bedrock_runtime = setup_bedrock()
    embeddings = BedrockEmbeddings(
        client=bedrock_runtime,
        model_id="amazon.titan-embed-text-v1",
    )
    
    client = get_mongo_client()
    collection = get_travel_collection(client)
    
    field_name_to_be_vectorized = "About Place"

    logger.info("Generating embeddings for query")
    text_as_embeddings = embeddings.embed_documents([query])
    embedding_value = text_as_embeddings[0]

    # get the vector search results based on the filter conditions.
    logger.info("Performing vector search in MongoDB")
    response = collection.aggregate(
        [
            {
                "$vectorSearch": {
                    "index": "travel_vector_index",
                    "path": "details_embedding",
                    "queryVector": embedding_value,
                    "numCandidates": 200,
                    "limit": 10,
                }
            },
            {
                "$project": {
                    "score": {"$meta": "vectorSearchScore"},
                    field_name_to_be_vectorized: 1,
                    "_id": 0,
                }
            },
        ]
    )

    # Result is a list of docs with the array fields
    docs = list(response)
    logger.info(f"Found {len(docs)} results from vector search")

    # Extract an array field from the docs
    array_field = [doc[field_name_to_be_vectorized] for doc in docs]

    # Join array elements into a string
    llm_input_text = "\n \n".join(str(elem) for elem in array_field)

    # utility
    newline, bold, unbold = "\n", "\033[1m", "\033[0m"
    logger.info(
        newline
        + bold
        + "Given Input From MongoDB Vector Search: "
        + unbold
        + newline
        + llm_input_text
        + newline
    )

    return llm_input_text

@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event: dict, context: LambdaContext):
    logger.info("Lambda handler invoked")
    return app.resolve(event, context)

def get_secret(secret_name):
    """
    Retrieve secret from AWS Secrets Manager
    """
    client = boto3.client(
        service_name='secretsmanager'
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        logger.error(f"Error retrieving secret {secret_name}: {e}")
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            logger.info(f"Successfully retrieved secret {secret_name}")
            return get_secret_value_response['SecretString']

def get_mongo_client():
    mongodb_uri = get_secret("workshop/atlas_secret")  # Replace with your secret name
    logger.info("Creating MongoDB client connection")
    client = MongoClient(mongodb_uri)
    return client


if __name__ == "__main__":  
    logger.info("Generating OpenAPI JSON schema")
    print(app.get_openapi_json_schema())
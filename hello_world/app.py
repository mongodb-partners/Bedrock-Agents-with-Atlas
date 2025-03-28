from time import time

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.event_handler import BedrockAgentResolver
from aws_lambda_powertools.utilities.typing import LambdaContext
from typing_extensions import Annotated
from aws_lambda_powertools.event_handler.openapi.params import Body, Query
from pymongo import MongoClient
import os

tracer = Tracer()
logger = Logger()
app = BedrockAgentResolver()


@app.get("/current_time", description="Gets the current time in seconds")  
@tracer.capture_method
def current_time() -> int:
    return int(time())

@app.get("/get_plot", description="retrieives a movie plot by its title")
@tracer.capture_method
def get_plot(title:  Annotated[str, Query(description="The title of the movie")]
            ) -> Annotated[str, Body(description="Whether the plot was retrieved successfully")]:
    
    client = get_mongo_client()
    logger.info("getting movite plot")
    # get database and collection
    db = client[os.environ.get('MONGO_DB')]
    collection = db[os.environ.get('MONGO_COLLECTION')]
    # update user data in MongoDB
    result = collection.find_one(
        {'title': title}
    )

    # return success message
    logger.info("got movie plot:" + str(result))
    return result.get('plot')


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event: dict, context: LambdaContext):
    return app.resolve(event, context)

def get_mongo_client():
    client = MongoClient(os.environ['MONGO_URI'])
    return client


if __name__ == "__main__":  
    print(app.get_openapi_json_schema())
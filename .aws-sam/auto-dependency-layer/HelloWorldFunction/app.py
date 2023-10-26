import boto3
import os

from aws_lambda_powertools.event_handler import APIGatewayHttpResolver
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools import Logger
from aws_lambda_powertools import Tracer
from aws_lambda_powertools import Metrics
from aws_lambda_powertools.metrics import MetricUnit
from amazondax import AmazonDaxClient

app = APIGatewayHttpResolver()
tracer = Tracer()
logger = Logger()
metrics = Metrics(namespace="Powertools")

ddb = None
ddb_table = None
ddb_table_name = os.getenv("DDB_TABLE_NAME")

@app.get("/hello")
@tracer.capture_method
def hello():
    # adding custom metrics
    # See: https://awslabs.github.io/aws-lambda-powertools-python/latest/core/metrics/
    metrics.add_metric(name="HelloWorldInvocations", unit=MetricUnit.Count, value=1)

    # structured log
    # See: https://awslabs.github.io/aws-lambda-powertools-python/latest/core/logger/
    logger.info("Hello world API - HTTP 200")
    return {"message": "😂🤣😂"}

@app.get("/bin")
@tracer.capture_method
def hello():
    # adding custom metrics
    # See: https://awslabs.github.io/aws-lambda-powertools-python/latest/core/metrics/
    metrics.add_metric(name="BinInvocations", unit=MetricUnit.Count, value=1)

    # structured log
    # See: https://awslabs.github.io/aws-lambda-powertools-python/latest/core/logger/
    logger.info("Bin API - HTTP 200")
    return {"message": "😂🤣😂"}

def init_ddb():
    global ddb
    global ddb_table

    dax_endpoint = os.getenv("DAX_ENDPOINT")
    if ddb is None:
        if dax_endpoint is not None:
            ddb = AmazonDaxClient.resource(endpoint_url=dax_endpoint)
        else:
            ddb = boto3.resource("dynamodb")

    if ddb_table is None:
        ddb_table = ddb.Table(ddb_table_name)

# Enrich logging with contextual information from Lambda
@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_HTTP)
# Adding tracer
# See: https://awslabs.github.io/aws-lambda-powertools-python/latest/core/tracer/
@tracer.capture_lambda_handler
# ensures metrics are flushed upon request completion/failure and capturing ColdStart metric
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)

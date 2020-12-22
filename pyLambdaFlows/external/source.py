import json
import pickle
from time import sleep
import boto3 
from pyLambdaFlows.decorator import kernel

@kernel
def lambda_handler(inputData):
    return inputData

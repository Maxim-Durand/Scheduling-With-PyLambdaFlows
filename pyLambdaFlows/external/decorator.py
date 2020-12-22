import boto3
import json
from time import sleep,time
import random
import pickle
import decimal
import sys
import traceback
def isIterable(obj):
    try:
        _ = iter(obj)
    except TypeError:
        return False
    return True
def put_data(client, table_name, indexData, obj):
    table = client.Table(table_name)
    response = table.update_item(Key={
        'id' : int(indexData)
    },
    UpdateExpression="set #data = :val",
    ExpressionAttributeValues={
        ':val' : pickle.dumps(obj)
    },
    ExpressionAttributeNames={
    "#data": "data"
    },
    ReturnValues="UPDATED_NEW")


def get_data(client, table_name, indexData):
    return pickle.loads(client.get_item(TableName=table_name, Key= { "id" : {"N" : str(indexData)}})["Item"]["data"]["B"])

def put_entry(client, table_name, idx, data, remaining):
    client.put_item(TableName=table_name, Item={
        "id" : {"N" : str(idx)},
        "remaining" : { "N" : str(remaining)},
        "data" : { "B" : pickle.dumps(data) }
    })

def get_entry(client, table_name, idx):
    result = client.get_item(TableName=table_name, Key= { "id" : {"N" : str(idx)}})["Item"]
    return int(result["id"]["N"]), int(result["remaining"]["N"]), pickle.loads(result["data"]["B"])


def kernel(func):
#Le wrapper permet d'acceder aux arguments de la function decore
    def wrapper(event, context):
        #pre traitement
        # Get all event data
        idx   = event["idx"]
        source = event["source"]
        data = event["data"]
        children = event["children"]
        dynamodb_table = event["table"]

        dynamoDbClient = boto3.client('dynamodb')
        dynamoDbRessource = boto3.resource('dynamodb')
        multi_parent = False
        if(source=='direct'):
            inputData = [pickle.loads(bytes(bytearray.fromhex(element))) for element in data]
        if(source=='data'):
            inputData = list()
            
            for idx_loc in data:
                if not isinstance(idx_loc, str):
                    multi_parent = True
                    batchResult = list()
                    for sub_idx in idx_loc:
                        batchResult.append(get_data(dynamoDbClient, dynamodb_table, sub_idx))
                    if len(batchResult)==1:
                        batchResult = batchResult[0]
                    inputData.append(batchResult)   
                else:
                    batchResult = get_data(dynamoDbClient, dynamodb_table, idx_loc)
                    inputData.append(batchResult)   
        #execution du code
        try:
            if not multi_parent:
                if len(inputData)==1:
                    result = func(inputData[0])
                else:
                    result = func(inputData)
            else:
                result = func(*inputData)

        except Exception:
            etype, value, tb = sys.exc_info()
            error_list = get_entry(dynamoDbClient, dynamodb_table, -1)[-1]
            error_list.append( (idx, etype, value, traceback.extract_tb(tb)))
            put_entry(dynamoDbClient, dynamodb_table, -1, error_list, 1)
            return {
                'statusCode': 200,
                'body': json.dumps("Ok")
            }   
        #post traitement
        # Store
        try:
            put_data(dynamoDbRessource, dynamodb_table, idx, result)
        except Exception:
            etype, value, tb = sys.exc_info()
            error_list = get_entry(dynamoDbClient, dynamodb_table, -1)[-1]
            error_list.append( (idx, etype, value, traceback.extract_tb(tb)))
            put_entry(dynamoDbClient, dynamodb_table, -1, error_list, 1)
            return {
                'statusCode': 200,
                'body': json.dumps("Ok")
            }  
            
        if(get_entry(dynamoDbClient, dynamodb_table, -1)[1] != 0):
            return {
                'statusCode': 200,
                'body': json.dumps("Ok")
            }   
        # Treatment
        if(len(children.keys()) != 0):

            for _, item in children.items():
                # Decremente 

                client = boto3.resource("dynamodb")
                table = client.Table(dynamodb_table)
                response = table.update_item(Key={
                    'id' : int(item["idx"])
                },
                UpdateExpression="set remaining = remaining - :val",
                ExpressionAttributeValues={
                    ':val' : decimal.Decimal(1)
                },
                ReturnValues="UPDATED_NEW")
                child_remaining_it = int(response["Attributes"]["remaining"])

                # call
                if child_remaining_it==0:
                    lambda_client = boto3.client('lambda')
                    lambda_client.invoke(
                    FunctionName=item['func'],
                    InvocationType='Event',
                    Payload=json.dumps(item),
                    )
    
        return {
            'statusCode': 200,
            'body': json.dumps("Ok")
        }   
    return wrapper
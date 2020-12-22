import botocore 
from .session import get_default_session
from .error import NoSessionGiven
import boto3
import decimal
import pickle

def reset_table(table_name, sess=None):
    """Reset a dynamodb table according to pylambdaflow specifications.

    This method delete the dynamodb table is it already exist under given name.
    Then it recreate it with the given name and pylambdaflow specifications.

    :param str table_name: Dynamodb name to use.
    :param Session sess:  Session to use, if not provided it will use the default one. 

    :raises: :class:`NoSessionGiven` : No session given
    """
    if sess is None:
        sess = get_default_session()
    if sess is None:
        raise NoSessionGiven()

    # delete if exist
    delete_table(table_name, sess=sess)
    create_table(table_name, sess=sess)


def table_exists(table_name, sess=None):
    """Return if the table exist.

    This function look at AWS API to return if the dynamodb table already exist.

    :param str table_name: Dynamodb name to use.
    :param Session sess:  Session to use, if not provided it will use the default one. 

    :raises: :class:`NoSessionGiven` : No session given
    """
    if sess is None:
        sess = get_default_session()
    if sess is None:
        raise NoSessionGiven()

    client = sess.getDynamoDb()
    clientRessource = sess.getDynamoDbRessource()
    table = clientRessource.Table("pyLambda")
    try:
        table.table_status
    except client.exceptions.ResourceNotFoundException:
        return False
    return True


def delete_table(table_name, sess=None, wait=True):
    """Delete the dynamodb table on AWS server.

    This function will delete the dynamodb table using either the specified or the default session.

    :param str table_name: Dynamodb name to use.
    :param Session sess:  Session to use, if not provided it will use the default one. 
    :param bool wait: Should the function wait for the table removing.

    :raises: :class:`NoSessionGiven` : No session given
    """
    if sess is None:
        sess = get_default_session()
    if sess is None:
        raise NoSessionGiven()

    client = sess.getDynamoDb()
    clientRessource = sess.getDynamoDbRessource()
    while True:        
        try:
            table = clientRessource.Table(table_name)
            table.delete()
            if wait:
                table.wait_until_not_exists()
        except client.exceptions.ResourceInUseException:
            continue
        except client.exceptions.ResourceNotFoundException:
            break


def decremente(table_name, idx, offset=1, sess=None):
    """Decrement the remaning field on the given id.

    This function will decrease the remaining var on the specified dynamodb table.
    It will return the actual value of this var according to the asked operation.

    :param str table_name: Dynamodb name to use.
    :param int idx: key to deal with.
    :param Session sess:  Session to use, if not provided it will use the default one. 

    :raises: :class:`NoSessionGiven` : No session given
    """
    if sess is None:
        sess = get_default_session()
    if sess is None:
        raise NoSessionGiven()

    client = sess.getDynamoDbRessource()
    table = client.Table(table_name)
    response = table.update_item(Key={
        'id' : idx
    },
    UpdateExpression="set remaining = remaining - :val",
    ExpressionAttributeValues={
        ':val' : decimal.Decimal(offset)
    },
    ReturnValues="UPDATED_NEW")
    return int(response["Attributes"]["remaining"])



def create_table(table_name, sess=None, wait=True):
    """Create a dynamodb table on AWS server with the specified name.

    This function will create the dynamodb table using either the specified or the default session.
    The table will be created using the pyLambdaFlow specifications.

    :param str table_name: Dynamodb name to use.
    :param Session sess:  Session to use, if not provided it will use the default one. 
    :param bool wait: Should the function wait for the table removing.

    :raises: :class:`NoSessionGiven` : No session given
    """
    if sess is None:
        sess = get_default_session()
    if sess is None:
        raise NoSessionGiven()

    client = sess.getDynamoDb()
    clientRessource = sess.getDynamoDbRessource()

    table = clientRessource.Table(table_name)

    client.create_table(TableName=table_name,
        KeySchema=[ {
                    "AttributeName" : "id",
                    "KeyType" : "HASH"
                }],
        AttributeDefinitions=[
            {
                "AttributeName" : "id",
                "AttributeType" : "N"
            }],
        ProvisionedThroughput={
            'ReadCapacityUnits' :  25,
            'WriteCapacityUnits' : 25
        })
    if wait:
        table.wait_until_exists()



def fill_table(table_name, counter_init, sess=None):
    """Update the whole table for computation.

    This function will write on all required line on the table in order to prepare
    the computation over lambda call. The counter_init must be the array to all futur
    lambda call with the accocieted parent number.
    For instance, this counter_init array ([0,0,2]) means that the 2 firsts elements
    haven't parent contrary to the last one.

    :param str table_name: Dynamodb name to use.
    :param list counter_init: The remaining field to use per table entry.
    :param Session sess:  Session to use, if not provided it will use the default one. 

    :raises: :class:`NoSessionGiven` : No session given
    """
    if sess is None:
        sess = get_default_session()
    if sess is None:
        raise NoSessionGiven()

    client = sess.getDynamoDb()
    for idx, init_val in enumerate(counter_init):
        res = client.put_item(TableName=table_name, Item={
            "id" : {"N" : str(idx)},
            "remaining" : { "N" : str(init_val)},
            "data" : { "B" : pickle.dumps(None) }
        },
        ReturnConsumedCapacity="TOTAL")
        print(res)
        

def put_entry(table_name, idx, data, remaining, sess=None):
    """Update one entry on the `table_name` table.

    This function will update on entry on the dynamodb table according to given args.

    :param str table_name: Dynamodb name to use.
    :param int idx: key entry to deal with.
    :param data: Data to put on specified entry.
    :param int remaining: Remaning value to use.

    :raises: :class:`NoSessionGiven` : No session given
    """
    if sess is None:
        sess = get_default_session()
    if sess is None:
        raise NoSessionGiven()

    client = sess.getDynamoDb()
    client.put_item(TableName=table_name, Item={
        "id" : {"N" : str(idx)},
        "remaining" : { "N" : str(remaining)},
        "data" : { "B" : pickle.dumps(data) }
    })


def get_entries_group(table_name, bottom, top, sess=None):
    """Return the entries according to the given dynamodb table and the index interval.

    This function provide the actual values from the bottom idx to the upper one. 
    If you specified bottom = 1 and top=4, this function will return the following indexes :
    1, 2, 3, 4.

    :param str table_name: Dynamodb table name to use.
    :param int bottom: Lower index to return.
    :param int top: Upper index to return. 
    """
    return get_entries_list(table_name, list(range(bottom, top+1)), sess=sess)


def get_entries_list(table_name, idx_list, sess=None):
    """Return the entries according to the given dynamodb table and the index list.

    This function provide the actual values specified on the idx list. 

    :param str table_name: Dynamodb table name to use.
    :param list idx_list: Index list to deal with (int list).
    """
    if sess is None:
        sess = get_default_session()
    if sess is None:
        raise NoSessionGiven()
    result = list()
    while len(idx_list)>40:
        result.extend(get_entries_list(table_name, idx_list[:40], sess=sess))
        idx_list = idx_list[40:]

    json_dict = {table_name : {'Keys' : [ {"id": element} for element in idx_list],
                               'ConsistentRead' : True}}

    client = sess.getDynamoDbRessource()
    res = client.batch_get_item(RequestItems=json_dict)["Responses"]["pyLambda"]
    parsed_res = [(int(element["id"]), pickle.loads(element["data"].value), int(element["remaining"])) for element in res]
    result.extend(parsed_res)
    return list(sorted(result, key=lambda element : element[0]))

def get_entry(table_name, idx, sess=None):
    """Return the entry according to dynamodb table name and index entry.

    This function provide the actual value of the specified entry (according 
    to the dynamodb table name).

    :param str table_name: Dynamodb name to use.
    :param int idx: key entry to deal with.
    :param int remaining: Remaning value to use.

    :return: The entry as a tuple (idx, remaining, data).

    :raises: :class:`NoSessionGiven` : No session given
    """
    if sess is None:
        sess = get_default_session()
    if sess is None:
        raise NoSessionGiven()

    client = sess.getDynamoDb()
    result = client.get_item(TableName=table_name, Key= { "id" : {"N" : str(idx)}})["Item"]
    return int(result["id"]["N"]), int(result["remaining"]["N"]), pickle.loads(result["data"]["B"])


def put_data(table_name, indexData, obj, sess=None):
    """Update the serialized data on the specified dynamodb entry.

    This function update the data field on the specified dynamodb table name and the specified entry.

    :param str table_name: Dynamodb name to use.
    :param int indexData: key entry to deal with.
    :param int obj: Data to put in.

    :raises: :class:`NoSessionGiven` : No session given
    """
    if sess is None:
        sess = get_default_session()
    if sess is None:
        raise NoSessionGiven()

    client = sess.getDynamoDbRessource()
    table = client.Table(table_name)
    response = table.update_item(Key={
        'id' : indexData
    },
    UpdateExpression="set #data = :val",
    ExpressionAttributeValues={
        ':val' : pickle.dumps(obj)
    },
    ExpressionAttributeNames={
    "#data": "data"
    },
    ReturnValues="UPDATED_NEW")


def get_data(table_name, indexData, sess=None):
    """Return the accocieted serialized object according to the entry index and the table name. 

    This function provide the accual object on the table entry.

    :param str table_name: Dynamodb name to use.
    :param int indexData: key entry to deal with.

    :return: The entry's data.
    
    :raises: :class:`NoSessionGiven` : No session given
    """
    if sess is None:
        sess = get_default_session()
    if sess is None:
        raise NoSessionGiven()
    
    client = sess.getDynamoDb()
    return pickle.loads(client.get_item(TableName=table_name, Key= { "id" : {"N" : str(indexData)}})["Item"]["data"]["B"])
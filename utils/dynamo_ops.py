from aws import client

ddb = client.DynamoDB(region_name='').get_client()

def update_is_extracted(table_name, key, is_extracted):
    ddb.update_item(TableName=table_name,
                    Key={
                        'name': {'S': key}
                    },
                    UpdateExpression='SET is_extracted = :is_extracted',
                    ExpressionAttributeValues={
                        ':is_extracted': {'BOOL': is_extracted},
                    })


def update_is_transformed(table_name, key, is_transformed):
    ddb.update_item(TableName=table_name,
                    Key={
                        'name': {'S': key}
                    },
                    UpdateExpression='SET is_transformed = :is_transformed',
                    ExpressionAttributeValues={
                        ':is_transformed': {'BOOL': is_transformed},
                    })


def get_value(table_name, key, value):
    return ddb.get_item(TableName=table_name,
                        Key={
                            key: {'S': value}
                        })['Item']

def scan(table_name):
    return ddb.scan(TableName=table_name)['Items']

def update_item(table_name, key, value_key, new_value, new_value_type):
    ddb.update_item(TableName=table_name,
                    Key={
                        'task': {'S': key}
                    },
                    UpdateExpression=f'SET {value_key} = :new_value',
                    ExpressionAttributeValues={
                        ':new_value': {new_value_type: new_value},
                    })


def init_artist(table_name, id, name):
    ddb.put_item(TableName=table_name,
                      Item={
                          'id': {'S': id},
                          'name': {'S': name},
                          'is_extracted': {'BOOL': False},
                          'is_transformed': {'BOOL': False}
                      })

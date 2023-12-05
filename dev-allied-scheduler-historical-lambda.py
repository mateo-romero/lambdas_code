import boto3
from datetime import datetime, timedelta
import dateutil
import json
import os
import traceback
from catch_error import catch_error

def lambda_handler(event, context):
    
    try:
        dynamodb_client = boto3.client("dynamodb")
        sqs_client = boto3.client("sqs")
        
        '''Obtenemos los registron de las programaciones de historicos aliados que estan en estado pendiente ''' 
        historical_query_statement = ''' SELECT * FROM aud_load_external_files WHERE process_type = 'historical' and state = 'PENDIENTE' '''
        
        dict_historical_response_query = dynamodb_client.execute_statement(Statement=historical_query_statement)
        dict_historical_response_query = dict_historical_response_query['Items']
        
        for historical_reload_request in dict_historical_response_query:
            print(historical_reload_request)
            '''con los datos del registro de programacion historical generamos el pk y sk para hacer 
            la query que nos permite obtener el path del aliado para enviar los historicos '''
            pk = 'country#' + historical_reload_request["country"]['S'] + '#id_allied#' + historical_reload_request["id_allied"]['S']
            sk = 'ean_provider#' + historical_reload_request["ean_provider"]['S'] + '#ean_trader#' + historical_reload_request["ean_trader"]['S']

            historical_path_query_statement = ''' SELECT * FROM conf_allied
                        WHERE pk = '{0}' and sk = '{1}' '''.format(pk, sk)
            historical_path_response_query = dynamodb_client.execute_statement(Statement=historical_path_query_statement)
            historical_path_response_query = historical_path_response_query['Items'][0]['historic_path']['S']
            
            
            historical_reload_request["historic_path"] = {"S": historical_path_response_query}
            
            sqs_client.send_message(
                QueueUrl=os.environ['sqs'],
                MessageBody=json.dumps(historical_reload_request)
            )
            
            change_state_dynamo(historical_reload_request,'PROCESANDO')

        return {'statusCode': 200}
    
    except:
        '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
        e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
        catch_error(traceback, context, dateutil, datetime, boto3)
        raise
    
    

def change_state_dynamo(item,state):
    DYNAMODB_CLIENT = boto3.client("dynamodb")

    pk = item['pk']['S'] 
    sk = item['sk']['S']
    
    response = DYNAMODB_CLIENT.update_item(
        TableName='conf_allied',
        Key={'pk': {'S': pk},'sk': {'S': sk}},
        UpdateExpression='SET #s = :val1',
        ExpressionAttributeNames={'#s': 'state'},
        ExpressionAttributeValues={':val1': {'S': state }}
    )
    
    return response
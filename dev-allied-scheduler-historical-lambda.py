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
        historical_query_statement = ''' SELECT * FROM conf_allied WHERE begins_with("sk", 'id_request#') and state = 'PENDIENTE' '''
        
        dict_historical_response_query = dynamodb_client.execute_statement(Statement=historical_query_statement)
        dict_historical_response_query = dict_historical_response_query['Items']
        
        for historical_message in dict_historical_response_query:
            historical_message["type_process"] = {"S": "historical"}
            print(historical_message)
            '''con los datos del registro de programacion historical generamos el pk y sk para hacer 
            la query que nos permite obtener el path del aliado para enviar los historicos '''
            pk = 'country#' + historical_message["country"]['S'] + '#id_allied#' + historical_message["id_allied"]['S']
            sk = 'ean_provider#' + historical_message["ean_provider"]['S'] + '#ean_trader#' + historical_message["ean_trader"]['S']

            historical_path_query_statement = ''' SELECT * FROM conf_allied
                        WHERE pk = '{0}' and sk = '{1}' '''.format(pk, sk)
            
            historical_path_response_query = dynamodb_client.execute_statement(Statement=historical_path_query_statement)
            print(historical_path_response_query)
            historical_path_response_query = historical_path_response_query['Items'][0]['historic_path']['S']
            historical_message["historic_path"] = {"S": historical_path_response_query}
            
            '''convertimos el atributo star_date y end_date para poder recorrer los dias correspondientes a esas dos fechas
            por cada dia agregamos el atributo sale_date y enviamos el registro a la cola 
            '''
            start_date = datetime.strptime(historical_message["start_date"]["S"][:10], "%Y-%m-%d")
            end_date = datetime.strptime(historical_message["end_date"]["S"][:10], "%Y-%m-%d")

            while start_date <= end_date:
                print(start_date)
                historical_message["sale_date"] = {"S": start_date.strftime("%Y%m%d")}
                start_date += timedelta(days=1)
                sqs_client.send_message(
                    QueueUrl=os.environ['sqs'],
                    MessageBody=json.dumps(historical_message)
                )
  

        return {'statusCode': 200}
    
    except:
        '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
        e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
        catch_error(traceback, context, dateutil, datetime, boto3)
        raise
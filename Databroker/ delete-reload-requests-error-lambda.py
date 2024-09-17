import boto3
import csv
import json
import os
from io import StringIO
from datetime import datetime
import traceback
import dateutil
from catch_error import catch_error


# Connect to Dynamodb
dynamo_client = boto3.client('dynamodb')

# Connect to Athena
athena = boto3.client('athena')

# Connect to S3
s3_client = boto3.client('s3')

timeZone = dateutil.tz.gettz('America/Bogota')
date = (datetime.now(tz=timeZone)).strftime('%Y-%m-%d %H:%M:%S')



'''
Función principal
'''
def lambda_handler(event, context):
    
    try: 
        evento_cause =  json.loads(event["Cause"])
        errorMessage = evento_cause["ErrorMessage"]
        jobName = evento_cause["JobName"]
        facts_table = None
        
        if('sales' in jobName):
          facts_table = "facts_sales"
          bucket_folder = 'stg_sales/'
          log_file_name , path_log_file_name = creation_file_error_csv(errorMessage,'sales')

           #Obtenemos las solicitudes de Borrado 
          response_request = get_requests('sales')

        elif ('stock' in jobName):
          facts_table = "facts_stock" 
          bucket_folder = 'stg_stock/'
          log_file_name , path_log_file_name = creation_file_error_csv(errorMessage,'stock')
          
          #Obtenemos las solicitudes de Borrado 
          response_request = get_requests('stock')
        
        for item in response_request:
          path_bucket_name = 'analitica-' + os.environ['environment'] + '-prescriptiva-stage-bucket'
          path_file = bucket_folder + item['file_name']['S'].replace(".EDI", "_R.parquet")
          
          rollback(item, facts_table)
          
          s3_client.delete_object(Bucket=path_bucket_name, Key=path_file)
          
          change_state_dynamo(item,'ERROR',log_file_name,path_log_file_name)
        
        
        return {'statusCode': 200}
    
    except:
        
        '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
        e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
        catch_error(traceback, context, dateutil, datetime, boto3)
        raise


'''
Función que permite crear un archivo de error en formato csv y almacenarlo en el bucket de error
'''
def creation_file_error_csv(error_message,type_document):
    
    # Crear un archivo CSV en memoria
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow([error_message])
    
    date_csv = datetime.now(tz=timeZone)
    formatted_date_time = date_csv.strftime("%Y%m%d%H%M%S")

    # Nombre del archivo CSV
    log_file_name = formatted_date_time + '_log.csv'

    # Subir el archivo CSV al bucket de S3
    bucket_name = 'analitica-' + os.environ['environment'] + '-prescriptiva-logs-bucket' 
    path_log_file_name = bucket_name + '/delete_and_reload/' + type_document + '/reload'
    s3_key = 'delete_and_reload/' + type_document + '/reload/' + log_file_name

    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    
    return log_file_name , path_log_file_name
        

'''
Función que permite obtener las solicitudes del borrado
'''
def get_requests(request_type):

    params = {
        'TableName': 'gen_delete_and_reload_requests',
        'KeyConditionExpression': 'pk = :a and begins_with(sk, :b)', 
        'FilterExpression': '#statu = :c',
        'ExpressionAttributeValues': {':a': { 'S': 'request_type#RECARGA', }, ':b': {'S': 'document_type#' + request_type,},':c': {'S': 'PENDIENTE',},},
        'ExpressionAttributeNames':{'#statu':  'state' }
    }

    response = dynamo_client.query(**params)
    items = response['Items']
    

    # Continuar haciendo llamadas mientras haya más resultados y exista un token de paginación
    while 'LastEvaluatedKey' in response:
        params['ExclusiveStartKey'] = response['LastEvaluatedKey']
        response = dynamo_client.query(**params)
        items.extend(response['Items'])

    return items


''' 
Función que permite hacer el rollback y cambiar el estado en dynamodb 
'''
def rollback(item, facts_table):

    load_date = item['load_date']['S'] 
    snrf = item['snrf']['S'] 
    bgm_document = item['bgm']['S'] 
    ean_provider =item['ean_provider']['S'] 
    ean_trader = item['ean_trader']['S'] 
    country = item['country']['S'] 
    query_select = None

        
    query_athena_delete = """DELETE  FROM "dwh_prescriptiva"."{6}"
                    where "ean_provider" = '{0}' 
                    and  "ean_trader" = '{1}'
                    and "snrf" = '{2}' 
                    and "bgm_document" = '{3}'
                    and "load_date" = '{4}'
                    and "country" = '{5}' """.format(ean_provider, ean_trader, snrf, bgm_document, load_date, country, facts_table)


    if("facts_sales" == facts_table):
        query_select = "ean_provider,ean_trader,sale_date,load_date,ean_point_sale,ean_product,total_price,unit_price,quantity,bgm_document,snrf,country,year,month,day"
    else:
        query_select = "ean_provider,ean_trader,stock_date,load_date,ean_point_sale,ean_product,net_total_price,net_unit_price,list_total_price,list_unit_price,quantity,quantity_in_transit,quantity_ean_com,trader_internal_code,provider_internal_code,item_description,special_condition,bgm_document,snrf,country,year,month,day"

    query_athena_rollback = """INSERT INTO "dwh_prescriptiva"."{6}"
                    SELECT {7}
                    FROM "dwh_prescriptiva"."{6}"
                    FOR TIMESTAMP AS OF (current_timestamp  - INTERVAL '30' MINUTE)
                    where "ean_provider" = '{0}' 
                    and  "ean_trader" = '{1}'
                    and "snrf" = '{2}' 
                    and "bgm_document" = '{3}'
                    and "load_date" = '{4}'
                    and "country" = '{5}'
                    ; """.format(ean_provider, ean_trader, snrf, bgm_document, load_date, country, facts_table, query_select)
                            
    # Ejecutar la sentencia SQL DELETE
    response = athena.start_query_execution(
        QueryString = query_athena_delete,
        QueryExecutionContext={
            'Database': 'dwh_prescriptiva'
        },
        ResultConfiguration={
            'OutputLocation': 's3://analitica-'+ os.environ['environment'] +'-prescriptiva-aws-bucket/athena/ '
        }
    )

    # Obtener el ID de la consulta del borrado
    query_delete_execution_id = response['QueryExecutionId']
    
    # Verificar si la consulta ha terminado correctamente
    if 'SUCCEEDED' == get_status_query(query_delete_execution_id, 'RUNNING'):
    
        # Ejecutar la sentencia SQL DELETE
        response_time_stamp = athena.start_query_execution(
            QueryString = query_athena_rollback,
            QueryExecutionContext={
                'Database': 'dwh_prescriptiva'
            },
            ResultConfiguration={
                'OutputLocation': 's3://analitica-'+ os.environ['environment'] +'-prescriptiva-aws-bucket/athena/ '
            }
        )
        
    
        # Obtener el ID de la consulta del rollback
        query_rollback_execution_id = response_time_stamp['QueryExecutionId']
        
        if 'SUCCEEDED' != get_status_query(query_rollback_execution_id, 'RUNNING'):
            msj_error =  "Error al hacer el rollback(insert viaje en el tiempo) en la funcionalidad de borrado y recarga, favor ejecutar el siguiente query sobre la consola de athena: [" + query_athena_rollback + "], en caso de presentar algún error al ejecutar este query debemos recargar la información de manera manual por el sitio web teniendo en cuenta los siguientes datos [ean_provider:" + ean_provider + " ean_trader:" + ean_trader + " snrf:" + snrf + " bgm_document:" + bgm_document + " load_date:" + load_date + " country:" + country +"]"
            register_error_rollback(msj_error)
    else:
        msj_error =  "Error al hacer el rollback(delete) en la funcionalidad de borrado y recarga, favor ejecutar el siguiente query sobre la consola de athena: [" + query_athena_delete + "], en caso de presentar algún error al ejecutar este query debemos contactar al equipo de desarrollo"
        register_error_rollback(msj_error)



'''
Función que permite obtener el status de la consulta en athena
'''
def get_status_query(execution_id, status):
    while (status == 'RUNNING' or status == 'QUEUED'):
        response_time_stamp = athena.get_query_execution(QueryExecutionId = execution_id)
        status = response_time_stamp['QueryExecution']['Status']['State']
    
    return status



'''
Función que permite registrar un error sucedido en el rollback
'''
def register_error_rollback(msj_error):
    # Generamos el item de error para la tabla aud_services_errors
    item = {
        "pk": {"S": f"type_service#{'Lambda'}"},
        "sk": {"S": f"name_service#prescriptiva-lambda-delete-reload-requests-error#date#{date}"},
        "date": {"S": date},
        "error_message": {"S": msj_error},
        "name_service": {"S": "prescriptiva-lambda-delete-reload-requests-error"},
        "type_service": {"S": 'Lambda'},
        "state": {"S": 'Activo'}
    }

    # Insertar el item en la tabla aud_services_errors
    dynamo_client.put_item(TableName="aud_services_errors", Item=item)



'''
Función que permite cambiar el estado a la solicitud de borrado y recarga
'''
def change_state_dynamo(item, state, log_file_name, path_log_file_name):

    #Se obtiene el pk y el Sk que vienen del parametro item
    pk = item['pk']['S'] 
    sk = item['sk']['S'] 
        
    # Definición del query hacía la tabla gen_delete_and_reload_requests
    response = dynamo_client.update_item(
        TableName='gen_delete_and_reload_requests',
        Key={'pk': {'S': pk},'sk': {'S': sk}},
        UpdateExpression='SET #s = :val1, log_file_name = :val2, path_log_file_name = :val3  ',
        ExpressionAttributeNames={'#s': 'state'},
        ExpressionAttributeValues={':val1': {'S': state },':val2': {'S': log_file_name},':val3': {'S': path_log_file_name}}
    )
    
    return response


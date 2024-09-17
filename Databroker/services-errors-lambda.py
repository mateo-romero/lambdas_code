import dateutil
import boto3
from datetime import datetime
import traceback
from catch_error import catch_error


# Conexión con dynamodb 
dynamodb = boto3.client("dynamodb")


''' Función Principal '''
def lambda_handler(event, context):

    try: 

        # Se definen las variables que se insertarán en la tabla de gen_services_errors de dynamodb
        name_service = ''
        type_service = ''
        error_message = ''
        
        # Se obtiene el objeto de la fecha en que ocurre el error
        timeZone = dateutil.tz.gettz('America/Bogota')
        date = datetime.now(tz=timeZone)
        
        # Se obtiene la fecha en el formato yymmdd
        date_error = date.strftime('%Y%m%d')
    
        # Se obtiene la hora en el forma hhmmss
        time_error = date.strftime('%H%M%S')
        
        # Se define la variable que se usará para obtener los valores de las variables que se insertarán en dynamodb
        type_service_dict = event['source']
        detail_error = event['detail']
        
        if(type_service_dict == 'aws.glue'):
            type_service = 'glue'
            name_service = detail_error['jobName']
            error_message = detail_error['message']
       
        if(type_service_dict == 'aws.states'):
            type_service = 'step_function'
            name_service = detail_error['stateMachineArn']
            error_message = detail_error['cause']
        
        # Definir el item a insertar en DynamoDB
        item = {
            "pk": {"S": f"type_service#{type_service}"},
            "sk": {"S": f"name_service#{name_service}#date_error#{date_error}#time_error#{time_error}"},
            "date_error": {"S": date_error},
            "time_error": {"S": time_error},
            "error_message": {"S": error_message},
            "name_service": {"S": name_service},
            "type_service": {"S": type_service},
            "state": {"S": 'Activo'}
        }

        # Insertar el item en la tabla aud_services_errors
        dynamodb.put_item(TableName="aud_services_errors", Item=item)
        
        return {'statusCode': 200}
    
    except:
        
        '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
        e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
        catch_error(traceback, context, dateutil, datetime, boto3)
        raise

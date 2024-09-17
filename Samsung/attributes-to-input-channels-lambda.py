import json
import urllib.parse
import boto3
from datetime import datetime
import dateutil
import traceback
import os
from catch_error import catch_error



# Constantes
channel = 'SFTP'
format_date = '%Y-%m-%d %H:%M:%S'

# Timezone
timeZone = dateutil.tz.gettz('America/Bogota')



''' Función Principal '''
def lambda_handler(event, context):
    
     # Execution
    start_execution = ''
    
    # Document fields
    ean_sender = ''
    ean_receiver = ''
    country = ''
    service = ''
    folder = ''
    isMapping = 'N'
    file_name = None
    document_type = None
    
     # Se asigna fecha y hora de incio de procesamiento de la lambda
    start_execution = datetime.now(tz=timeZone).strftime(format_date)
    
    try:

        s3 = boto3.client('s3')
        client = boto3.client('sns')

        # Obtener el nombre del bucket
        name_bucket = event['Records'][0]['s3']['bucket']['name']

        # Obtener la ruta del archivo
        path_file_sftp = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

        # Dividir la ruta del archivo
        array_paths = path_file_sftp.split('/')

        # Validar si la ruta del archivo cumple con la estructura
        if len(array_paths) >= 8:
            folder = array_paths[7]
        
        # Validar si el archivo se cargó en la carpeta IN
        if folder == 'IN':
            
            # Obtener datos en la ruta
            (ean_sender, ean_receiver, country, service, file_name) = get_values_from_route(array_paths)
            
            # Verificamos si existe un mapeo para el archivo
            (isMapping, document_type) = exist_mapping(ean_sender, ean_receiver, country)
            
            # Verificar si se encontró un registor de mapeo para el archivo
            if isMapping == 'Y':
                
                # Obtener información del archivo
                (file_name, file_name_without_extension, extension_file) = structure_file_name(file_name, ean_sender, ean_receiver, document_type)
                
                # Crear el registro del archivo en load_auidt
                register_load_audit(country, file_name_without_extension, document_type, ean_receiver, ean_sender,path_file_sftp, start_execution, extension_file)

                # se mueve al bukcer de raw cuando se encuentra que cuenta con un mapeo
                s3.copy_object(CopySource={'Bucket': name_bucket, 'Key': path_file_sftp}, Bucket=os.environ['raw_bucket'], Key='external_files/SFTP' + '/' +document_type + '/'+ file_name)
                
                # Eliminamos del bucket el archivo procesado
                s3.delete_object(Bucket=name_bucket, Key=path_file_sftp)

        
            response = client.publish(
                TargetArn=os.environ['ARNSNS'],
                Message=json.dumps({'default': json.dumps(event, ensure_ascii=False)}),
                MessageStructure='json',
                MessageAttributes={
                                    'service': {
                                        'DataType': 'String',
                                        'StringValue': service
                                    },
                                    'folder': {
                                        'DataType': 'String',
                                        'StringValue': folder
                                    },
                                    'isMapping':{
                                        'DataType': 'String',
                                        'StringValue': isMapping
                                    }
                                }
            )
        
        print('key :{'+ path_file_sftp +'} services :'+service + ' folder :'+folder + ' isMapping :'+isMapping)
        
        return service, folder, isMapping
    except:  
        
        '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
        e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
        catch_error(traceback, context, dateutil, datetime, boto3)
        raise
    
    
''' Función para estructuras el nombre del archivo para moverlo a raw bucker '''    
def structure_file_name(file_name, ean_sender, ean_receiver, document_type):
    current_date = datetime.now(tz=timeZone)
    load_date = current_date.strftime('%Y%m%d')
    load_time = current_date.strftime('%H%M%S')
    
    # Formatear estructura acompañante nombre original del archivo
    structure_name =f'_{ean_sender}_{ean_receiver}_{document_type.upper()}_{load_date}{load_time}'
    file_name_without_extension = ""
    extension_file = ""

    # Validar extensión para formatear el nombre del archivo 
    if '.csv' in file_name or '.CSV' in file_name:
        file_name =file_name.replace('.csv',  structure_name + '.csv').replace('.CSV',  structure_name + '.CSV')
        file_name_without_extension = file_name.replace('.csv','').replace('.CSV',  '')
        extension_file = 'csv'
    elif '.xlsx' in file_name or '.XLSX' in file_name:
        file_name =file_name.replace('.xlsx',  structure_name + '.xlsx').replace('.XLSX',  structure_name + '.XLSX')
        file_name_without_extension = file_name.replace('.xlsx','').replace('.XLSX',  '')
        extension_file = 'xlsx'
    elif '.txt' in file_name or '.TXT' in file_name:
        file_name =file_name.replace('.txt',  structure_name + '.txt').replace('.TXT',  structure_name + '.TXT')
        file_name_without_extension = file_name.replace('.txt','').replace('.TXT',  '')
        extension_file = 'txt'

    return (file_name, file_name_without_extension, extension_file)


'''Función que obtiene información de la ruta del archivo'''
def get_values_from_route(array_paths):

    ean_sender = array_paths[5]    
    country_ean_receiver = array_paths[6]
    ean_receiver = country_ean_receiver.split('_')[1]
    country = country_ean_receiver.split('_')[0]
    service = array_paths[3]
    file_name = array_paths[8]

    return (ean_sender, ean_receiver, country, service, file_name)



''' Función que permite guardar auditoria de los EDI recibidos por parte del cliente '''
def register_load_audit(country,file_name_without_extension,document_type,ean_receiver,ean_sender,path_file_sftp,start_execution, extension_file):
    
    # Conexión con dynamodb
    dynamodb = boto3.client('dynamodb')

    dynamodb.put_item(
        TableName='aud_load_audit',
        Item={
            'pk': {'S': 'country#' + country + '#document_type#' + document_type},
            'sk': {'S': 'channel#' + channel + '#ean_provider#' + ean_receiver + '#ean_trader#' + ean_sender + '#file_name#' + file_name_without_extension},
            'bgm': {'S': ''},
            'country': {'S': country},
            'ean_provider': {'S': ean_receiver},
            'ean_trader': {'S': ean_sender},
            'file_name': {'S': file_name_without_extension},
            'load_date': {'S':  ''},
            'load_day': {'S': ''},
            'load_hour': {'S': ''},
            'load_month': {'S': ''},
            'load_year': {'S': ''},
            'path_file': {'S': ''},
            'provider': {'S': ''},
            'reported_end_date': {'S': ''},
            'reported_start_date': {'S': ''},
            'snrf': {'S': ''},
            'state': {'S': 'ACTIVO'},
            'trader': {'S': ''},
            'document_type': {'S': document_type},
            'processing_state': {'S': 'PENDIENTE'},
            'log_audit': {'S': ''},
            'start_execution': {'S': start_execution},
            'end_execution': {'S': ''},
            'total_records': {'S': ''},
            'file_name_log': {'S': ''},
            'path_file_log': {'S': ''},
            'path_file_sftp': {'S': path_file_sftp},
            'channel': {'S': channel},
            "email_load": {"S": "soporteprescriptiva@gmail.com" },
            'user_load': {'S': 'soporte'},
            'extension_file': {'S': extension_file},
            'details': {'L': []}
        },
        ReturnValues='NONE',
    )


'''Función que verifica si existe un mapeo para el archivo'''
def exist_mapping(ean_sender, ean_receiver, country):
    
    # Crear un recurso DynamoDB
    dynamodb_client = boto3.client('dynamodb')
    
    # Define la consulta PartiQL con parámetros
    query = "SELECT document_type FROM conf_mapping_external_files WHERE country = ? AND ean_sender = ? AND ean_receiver = ?"
    
    # Define los valores de los parámetros
    query_params = [
         {'S': country},
         {'S': ean_sender},
         {'S': ean_receiver}
    ]
    
    # Ejecuta la consulta
    response = dynamodb_client.execute_statement(
        Statement=query,
        Parameters=query_params
    )
    # Verificar si encontró un mapeo
    if len(response['Items']) > 0:
        document_type = response['Items'][0]['document_type']['S']
        return ('Y', document_type)
    else:
        return ('N','default')
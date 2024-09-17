import json
import urllib.parse
import boto3
from datetime import datetime
import dateutil
import traceback
import os

# Constantes
channel = 'SFTP'
format_date = '%Y-%m-%d %H:%M:%S'

# Timezone
timeZone = dateutil.tz.gettz('America/Bogota')



# Función principal
def lambda_handler(event, context):
    document_type = ''

    # Se asigna fecha y hora de incio de procesamiento de la lambda
    start_execution = datetime.now(tz=timeZone).strftime(format_date)

    try:
        # Conexión con s3
        s3_client = boto3.client('s3')
            
        # Obtener los datos del evento S3
        event_sns = json.loads(event['Records'][0]['Sns']['Message'])['Records'][0]['s3']

        # Obtener los atributos del mensaje
        message_attributes = event['Records'][0]['Sns']['MessageAttributes']

        # Obtener el tipo de documento
        document_type = message_attributes['document_type']['Value']

        # Extraemos nombre del bucket asociado
        name_bucket = event_sns['bucket']['name']
            
        # Extraer ruta del archivo SFTP
        path_file_sftp = urllib.parse.unquote_plus(event_sns['object']['key'], encoding='utf-8')
        
        # Obtener datos en la ruta
        info_route = get_values_from_route(path_file_sftp)

        # Agregar el tipo de documento a la información de la ruta
        info_route['document_type'] = document_type

        # Obtener información del archivo
        info_file = structure_file_name(info_route)

        # Estructurar la información del archivo
        # para ser registrado en la tabla de auditoría
        info_register = {
            'country': info_route['country'],
            'file_name_without_extension': info_file['file_name_without_extension'],
            'document_type': document_type,
            'ean_receiver': info_route['ean_receiver'],
            'ean_sender': info_route['ean_sender'],
            'path_file_sftp': path_file_sftp,
            'start_execution': start_execution,
            'extension_file': info_file['extension_file']
        }
        
        # Crear el registro del archivo en load_auidt
        register_load_audit(info_register)
        
        print("REGISTR")
        
                
        # se mueve al bucket de raw
        s3_client.copy_object(CopySource={'Bucket': name_bucket, 'Key': path_file_sftp}, Bucket=os.environ['raw_bucket'], Key='external_files/SFTP' + '/' +document_type + '/'+ info_route['file_name'])
        
        print("COPIE A RAW")

        
        s3_client.copy_object(CopySource={'Bucket': name_bucket, 'Key': path_file_sftp}, Bucket=os.environ['backup_bucket'], Key=path_file_backup + '/' + info_route['file_name'])

        print("COPIE A RAW BACKUP")


        print("ELIMINAR")
        
        # Eliminamos del bucket el archivo procesado
        s3_client.delete_object(Bucket=name_bucket, Key=path_file_sftp)
        print("elimine")
    except:
        
        print('Error al obtener los datos del evento S3')

        '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
        e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
        print(traceback)
        raise



'''Función que obtiene información de la ruta del archivo'''
def get_values_from_route(bucket_route):
    lst_route = bucket_route.split('/')

    ean_sender = lst_route[5]    
    country_ean_receiver = lst_route[6]
    ean_receiver = country_ean_receiver.split('_')[1]
    country = country_ean_receiver.split('_')[0]
    service = lst_route[3]
    folder = lst_route[7]
    file_name = lst_route[8]

    # Estructurar información de la ruta
    info_route = {
        'ean_sender': ean_sender,
        'ean_receiver': ean_receiver,
        'country': country,
        'service': service,
        'folder': folder,
        'file_name': file_name
    }
    
    return info_route


''' Función para estructuras el nombre del archivo para moverlo a raw bucker '''    
def structure_file_name(info_route):
    current_date = datetime.now(tz=timeZone)
    load_date = current_date.strftime('%Y%m%d')
    load_time = current_date.strftime('%H%M%S')
    
    # Formatear estructura acompañante nombre original del archivo
    structure_name =f'_{info_route['ean_sender']}_{info_route['ean_receiver']}_{info_route['document_type'].upper()}_{load_date}{load_time}'
    file_name_without_extension = ""
    extension_file = ""

    # Validar extensión para formatear el nombre del archivo 
    if '.csv' in info_route['file_name'] or '.CSV' in info_route['file_name']:
        file_name =info_route['file_name'].replace('.csv',  structure_name + '.csv').replace('.CSV',  structure_name + '.CSV')
        file_name_without_extension = info_route['file_name'].replace('.csv','').replace('.CSV',  '')
        extension_file = 'csv'
    elif '.xlsx' in info_route['file_name'] or '.XLSX' in info_route['file_name']:
        file_name =info_route['file_name'].replace('.xlsx',  structure_name + '.xlsx').replace('.XLSX',  structure_name + '.XLSX')
        file_name_without_extension = info_route['file_name'].replace('.xlsx','').replace('.XLSX',  '')
        extension_file = 'xlsx'
    elif '.txt' in info_route['file_name'] or '.TXT' in info_route['file_name']:
        file_name =info_route['file_name'].replace('.txt',  structure_name + '.txt').replace('.TXT',  structure_name + '.TXT')
        file_name_without_extension = info_route['file_name'].replace('.txt','').replace('.TXT',  '')
        extension_file = 'txt'

    # Estructurar información del archivo
    info_file = {
        'file_name': file_name,
        'file_name_without_extension': file_name_without_extension,
        'extension_file': extension_file
    }

    return info_file


''' Función que permite guardar auditoria de los EDI recibidos por parte del cliente '''
def register_load_audit(info_register):
    
    # Conexión con dynamodb
    dynamodb_client = boto3.client('dynamodb')

    dynamodb_client.put_item(
        TableName='aud_load_audit',
        Item={
            'pk': {'S': 'country#' + info_register['country'] + '#document_type#' +info_register['document_type']},
            'sk': {'S': 'channel#' + channel + '#ean_provider#' + info_register['ean_receiver'] + '#ean_trader#' + info_register['ean_sender'] + '#file_name#' + info_register['file_name_without_extension']},
            'bgm': {'S': ''},
            'country': {'S': info_register['country']},
            'ean_provider': {'S': info_register['ean_receiver']},
            'ean_trader': {'S': info_register['ean_sender']},
            'file_name': {'S': info_register['file_name_without_extension']},
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
            'document_type': {'S': info_register['document_type']},
            'processing_state': {'S': 'PENDIENTE'},
            'log_audit': {'S': ''},
            'start_execution': {'S': info_register['start_execution']},
            'end_execution': {'S': ''},
            'total_records': {'S': ''},
            'file_name_log': {'S': ''},
            'path_file_log': {'S': ''},
            'path_file_sftp': {'S': info_register['path_file_sftp']},
            'channel': {'S': channel},
            "email_load": {"S": "soporteprescriptiva@gmail.com" },
            'user_load': {'S': 'soporte'},
            'extension_file': {'S': info_register['extension_file']},
            'details': {'L': []}
        },
        ReturnValues='NONE',
    )

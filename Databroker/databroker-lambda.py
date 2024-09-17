import json
import os
import requests
import traceback
from catch_error import catch_error
import boto3
import dateutil
from datetime import datetime


'''
Metodo principal de la Funcion lambda 
encargada del manejo de facturacion electronica
'''
def lambda_handler(event, context):

    # creamos un diccionario con las carpetas para tipos documento que posiblemente pueden llegar
    dict_document_types = {
        'FV' : 'invoice',
        'NC' : 'credit_note',
        'AE' : 'express_acceptance',
        'AT' : 'tacit_acceptance',
        'BS' : 'goods_service_receiver',
        'DEP': 'equivalent_document_pos',
        'DS' : 'supporting_document',
        'FC' : 'contingency_invoice',
        'FCD': 'contingency_invoice_dian',
        'FE' : 'export_invoice',   
        'NAD': 'adjustment_note_supporting_document',
        'ND' : 'debit_note',
        'NS' : 'supporting_note',
        'RA' : 'acknowledgement_of_receipt',
        'RF' : 'claim'
    }

    #se establece la zona horaria
    timeZone = dateutil.tz.gettz('America/Bogota')

    # fecha actual
    current_date = datetime.now(tz=timeZone)

    # diccionario con datos complemtarios que se utilizaran en el codigo
    dict_aud_load_audit_databroker = {
        'token_backend_plip': os.environ['token_backend_plip'],
        'url_backend_plip'  : os.environ['url_backend_plip'],
        'raw_bucket'   : os.environ['raw_bucket'],
        'backup_bucket'   : os.environ['backup_bucket'],
        'load_date'   : current_date.strftime('%Y%m%d'),
        'load_time'   : current_date.strftime('%H%M%S'),
        'file_name'      : '',
        'extension'      : '',
        'path_file'      : '',
        'processing_state': '',
        'message_error': ''
    }
    
    # se imprime el mensaje de la cola
    print('message_sqs >> ', event)

    #obtenemos el body de la sqs
    body_sqs = json.loads(event['Records'][0]['body'])
    
    # ontenemos el mensaje de la sns de feco
    message_sqs = json.loads(body_sqs['Message'])

    # obtenemos la carpeta tipo de documento
    dict_aud_load_audit_databroker['document_type'] = dict_document_types.get(validate_none_value(message_sqs.get('document').get('type')), validate_none_value(message_sqs.get('document').get('type')))
    
    try:

        # Crear un recurso S3
        s3_client = boto3.client('s3')

        # Crear un recurso DynamoDB
        dynamodb_client = boto3.client('dynamodb')

        # sacamos la extension del archivo y el nombre del archivo sin la extension
        dict_aud_load_audit_databroker['extension'] = os.path.splitext(message_sqs.get('document').get('filename'))[1][1:]

        # sacamos el nombre del archivo sin extension
        dict_aud_load_audit_databroker['file_name'] = message_sqs.get('document').get('filename').replace('.' + dict_aud_load_audit_databroker.get('extension'),'')

        # se inserta la traza  del archivo
        insert_update_load_audit(dict_aud_load_audit_databroker, message_sqs, dynamodb_client)

        # se obtiene el contenido del archivo que viene por la url prefimada
        file_content = get_file_to_url(message_sqs.get('document').get('presignedURL'))
     
        # verificamos si existe contenido
        if file_content != None:
        
            # cargamos el contenido al bucket raw de prescriptiva
            upload_file_bucket(file_content, dict_aud_load_audit_databroker, message_sqs, s3_client)

            # verificamos si el tipo de archico corresponde a uno e los que se tiene que enviar a plip
            if message_sqs.get('document').get('type') in ['FV', 'DEP', 'NC', 'ND', 'FCD']:
             
                #verificamos si el cliente a quien se le esta enviando la factura esta registrado
                if get_customer(message_sqs, dynamodb_client):

                    # enviamos el archivo al servicio de plip
                    response_plip = send_file(dict_aud_load_audit_databroker, message_sqs, s3_client)
                    
                    if response_plip.status_code == 201:

                        # agregamos el mensaje de envio correctamente
                        dict_aud_load_audit_databroker['processing_state'] = 'ENVIO EXITOSO'         
                    
                    else:
                        
                        # obtenemos la data de la respuesta del back
                        message_error = response_plip.json()

                        dict_aud_load_audit_databroker['processing_state'] = 'ERROR ENVIO'
                        dict_aud_load_audit_databroker['message_error'] = json.dumps(message_error)                     
                    
                else:
                    
                    # agregamos mensaje que el receiverId no existe
                    dict_aud_load_audit_databroker['processing_state'] = 'NO REGISTRADO'
            
            else:
                
                # agregamos mensaje que el tipo de archivo no se envia a plip
                dict_aud_load_audit_databroker['processing_state'] = 'ARCHIVO NO SOPORTADO'
        
        else:

            # mensaje de error si el archivo no tiene contenido
            dict_aud_load_audit_databroker['processing_state'] = 'ARCHIVO INVALIDO'

        # se inserta la traza  del archivo
        insert_update_load_audit(dict_aud_load_audit_databroker, message_sqs, dynamodb_client)

        return {'statusCode': 200}
        
    except:

        # se regsitra el error en la tabla de service_error
        catch_error(traceback, context, dateutil, datetime, boto3)
        raise



'''
Funcion que se encarga de buscar en la base de datos de clientes
si esta registrado se envia el archivo de la factura
@Params message_sqs datos del mensaje que envia desde la cola
'''
def get_customer(message_sqs, dynamodb_client):

    #declaro el sql que se enviara a la tabla
    query = " SELECT pk from gen_customer_databroker where pk = ? and sk = ?"

    # parametros para la consulta
    parameters = [
        {'S': f"service#{validate_none_value(message_sqs.get('petitioner')).upper()}#region#{validate_none_value(message_sqs.get('document').get('region')).upper()}"},
        {'S': f"document_number#{message_sqs.get('data').get('receiverId').strip()}"}
    ]

    # Ejecuta la consulta
    response_query = dynamodb_client.execute_statement(
        Statement=query,
        Parameters=parameters
    )

    return (len(response_query['Items']) > 0)



'''
Funcion que se encarga de registrar o actualizar en la tabla de auditoria
@Params dict_aud_load_audit_databroker diccionario con la data complemetaria para registrar traza
@Params message_sqs datos del mensaje enviado en la cola
@Params dynamodb_client cliente de conexion a base de datos dynamoDb
'''
def insert_update_load_audit(dict_aud_load_audit_databroker, message_sqs, dynamodb_client):

    dynamodb_client.put_item(
        TableName='aud_load_audit_databroker',
        Item={            
            'pk': {'S': f"receiver#{validate_none_value(message_sqs.get('data').get('receiverId')).strip()}"},
            'sk': {'S': f"sender#{validate_none_value(message_sqs.get('data').get('senderId')).strip()}#document_type#{validate_none_value(message_sqs.get('document').get('type'))}#cufe#{validate_none_value(message_sqs.get('data').get('cufe'))}"},
            'region':{'S': validate_none_value(message_sqs.get('document').get('region')).upper()},
            'receiver':{'S': validate_none_value(message_sqs.get('data').get('receiverId')).strip()},
            'email_receiver':{'S': validate_none_value(message_sqs.get('data').get('receiverEmail'))},
            'document_type':{'S': validate_none_value(message_sqs.get('document').get('type'))},
            'sender':{'S': validate_none_value(message_sqs.get('data').get('senderId')).strip()},
            'invoice_date':{'S': validate_none_value(message_sqs.get('data').get('issueDate'))},
            'cufe':{'S': validate_none_value(message_sqs.get('data').get('cufe'))},
            'file_name':{'S': dict_aud_load_audit_databroker.get('file_name')},
            'extension':{'S': dict_aud_load_audit_databroker.get('extension')},
            'path_file':{'S': dict_aud_load_audit_databroker.get('path_file')},
            'state': {'S': 'ACTIVO'},
            'load_date': {'S': dict_aud_load_audit_databroker.get('load_date')},
            'load_time': {'S': dict_aud_load_audit_databroker.get('load_time')},
            'processing_state':{'S': dict_aud_load_audit_databroker.get('processing_state')},       
            'message_error':{'S': dict_aud_load_audit_databroker.get('message_error')}
        }
    )



'''
Funcion que se encarga de cargar un archivo al bucket 
@Params file_content contenido del archivo
@Params dict_aud_load_audit_databroker datos adicionales como bucket y fechas
@Params message_sqs mensaje enviado por la cola
@Params s3_client cliente de conexion a S3
'''
def upload_file_bucket(file_content, dict_aud_load_audit_databroker, message_sqs, s3_client):

    # pasamos la fecha de factura a fecha valida
    invoice_date = datetime.strptime(message_sqs.get('data').get('issueDate'), '%Y-%m-%d')

    # generamos el path para guardar el archivo en el backup_bucket
    path_backup = '/'.join([
        dict_aud_load_audit_databroker.get('document_type'),    
        f'''region={message_sqs.get('document').get('region').upper()}''',
        f'''receiver={message_sqs.get('data').get('receiverId').strip()}''',
        f'''sender={message_sqs.get('data').get('senderId').strip()}''',
        f'''year={str(invoice_date.year)}''',
        f'''month={str(invoice_date.month).zfill(2)}''',
        f'''day={str(invoice_date.day).zfill(2)}'''        

    ])

    # agregamos al diccionario la ruta del backup
    dict_aud_load_audit_databroker['path_backup'] = path_backup
    dict_aud_load_audit_databroker['path_file'] = f"{dict_aud_load_audit_databroker.get('backup_bucket')}/{path_backup}"

    # se carga el archivo al bucket backup
    s3_client.upload_fileobj(file_content.raw, dict_aud_load_audit_databroker.get('backup_bucket'), f"{path_backup}/{message_sqs.get('document').get('filename')}")

    # si es factura lo pasamos al raw para generar el parquet
    if 'FV' == message_sqs.get('document').get('type'):

        #copiamos el archivo del backup para el raw para ser procesado
        s3_client.copy_object(CopySource={'Bucket': dict_aud_load_audit_databroker.get('backup_bucket'), 'Key': f"{path_backup}/{message_sqs.get('document').get('filename')}"},
                    Bucket=dict_aud_load_audit_databroker.get('raw_bucket'), Key=f"{dict_aud_load_audit_databroker.get('document_type')}/{message_sqs.get('document').get('filename')}")



'''
Funcion que se encarga de obtner el archivo guadado en el bucket
@Params dict_aud_load_audit_databroker datos para la carga del archivo
@Params datos del arhivo a guardar
@Params s3_client cliente de conexion a S3
'''
def get_file_bucket(dict_aud_load_audit_databroker, message_sqs, s3_client):

    # Obtenemos el file
    invoice_file = s3_client.get_object(Bucket=dict_aud_load_audit_databroker.get('backup_bucket'), Key=f"{dict_aud_load_audit_databroker.get('path_backup')}/{message_sqs.get('document').get('filename')}")
    
    # contenido del archivo
    file_content_invoice = invoice_file['Body'].read()
    
    return file_content_invoice
  
  
    
'''
Funcion que se encarga de enviar el archivo al servicio de plip
@Params dict_aud_load_audit_databroker diccionario con los datos complematrios de la aplicacion
@Params file_name nombre del archivo
@Params s3_client cliente de conexion a S3
'''
def send_file(dict_aud_load_audit_databroker, message_sqs, s3_client):
    
    #obtenemos el archivo xml en fisico
    file_content_invoice = get_file_bucket(dict_aud_load_audit_databroker, message_sqs, s3_client)
    
    #preparamos el objecto a enviar en la peticion
    data = {
        'documentNumber': message_sqs.get('data').get('receiverId').strip(),
        'email'         : message_sqs.get('data').get('receiverEmail')
    }
    
    files = {
        'invoice' : (message_sqs.get('document').get('filename'), file_content_invoice, 'application/xml')
    }
    
    # Agregar el token de autorización al encabezado de la solicitud
    headers = {'Authorization': dict_aud_load_audit_databroker.get('token_backend_plip')}
    
    #enviamos la solicitud y obtenemos la respuesta
    response_plip = requests.post(dict_aud_load_audit_databroker.get('url_backend_plip'), data=data, files=files, headers=headers)
    
    # regresamos la respuesta
    return response_plip



'''
Funcion que se encarga de consultar la url prefirmada y obtener el archivo
@Params url_file url del archivo
'''
def get_file_to_url(url_file):
    
    # hacer la peticion a la url
    response = requests.get(url_file,stream=True)
    
    # verifica si la peticion fue exitosa
    if response.status_code == 200:
        
        return response
        
    else:
        
        return None



'''
Esta función permite validar si el valor del parámetro "value" tiene el valor "None" y devuelve '' o "value" en caso contrario.
'''
def validate_none_value(value):

    return value if value is not None else ''
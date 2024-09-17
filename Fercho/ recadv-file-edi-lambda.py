import boto3
from pydifact.segmentcollection import RawSegmentCollection
import pandas as pd
from datetime import datetime
import dateutil
import urllib.parse
import awswrangler as wr
import os
import traceback
from catch_error import catch_error


# allow connection with some bucket
bucket_connection = boto3.client(service_name = 's3')

# Constantes
channel = 'CENT'
document_type = 'recadv'

'''Función princial de la lambda'''
def lambda_handler(event, context):
  
  try:
    
      # Estableciendo conexión a dynamodb
      dynamodb_client = boto3.client("dynamodb")
  
      # extract data from document put in bucket. In this case extract the name of bucket and document.
      name_bucket = event['Records'][0]['s3']['bucket']['name']
      
      path_file = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
      
      # Se hace el split por el delimitador "_"
      path_file_split_by_first_delimiter = path_file[:-4].split("_")
      
      path_file_split_by_second_delimiter = path_file.split("/")
      
      # Se obtiene el país en la última posición del nombre del archivo
      country = path_file_split_by_first_delimiter[len(path_file_split_by_first_delimiter)-1]
      
      # Se obtiene la fecha completa en la penúltima posición del nombre del archivo
      full_date = path_file_split_by_first_delimiter[len(path_file_split_by_first_delimiter)-2]
      
      # Se extrae el nombre del archivo
      file_name = path_file_split_by_second_delimiter[len(path_file_split_by_second_delimiter)-1]
      
      # Se obtiene la fecha YYYYMMDD
      load_date = full_date[:8]       # [LOAD_DATE]
      
      # Se obtiene la hora HHMM
      load_time = full_date[8:]      # [LOAD_TIME]
      
      # Se obtienen año, mes y día de carga con base en el campo load_date
      load_year = load_date[:4]
      load_month = load_date[-4:-2]
      load_day = load_date[-2:]
      
      # Variables temporales para almacenar la información del documento .EDI
      header_data = []
      detail_data = []
      data = []
      
      # Campos temporales referenciados en el encabezado inicializados en NONE
      head_unb_sender = None
      head_unb_receiver = None
      head_nad_by = None
      head_nad_su = None
      head_nad_dp = None
      head_bgm_doc_type = None
      head_bgm_recadv_number = None
      head_dtm_recadv_date = None
      head_dtm_receipt_date = None
      head_reference_document = None
      head_invoice_number = None
      head_dispatch_number = None
      head_folio_number = None
      head_serie_number = None
      head_uuid_number = None
      
      # Campos temporales referenciados en el detalle inicializados en NONE.
      det_ean_lin = None
      det_description_lin = None
      det_sent_quantity = None
      det_requested_quantity = None
      det_received_quantity = None
      det_accepted_quantity = None
      det_measure_unit = None
      det_bonus_quantity = None
      det_toreturn_quantity = None
      det_toreturn_change_reason = None
      det_todestroy_quantity = None
      det_pending_quantity = None
      det_excessive_quantity = None
      det_damaged_quantity = None
      det_difference_quantity = None
      det_total_value = None
      det_delivery_location = None
      det_reference_document = None
      
      # Campos temporales para el segmento UNZ
      unz_snrf = None
      
      # Variables para usar en la auditoría.
      log_audit = None
      processing_state = 'CARGA TOTAL'
      state = 'ACTIVO'
      total_records = 0
      start_execution = None
      end_execution = None
      provider = None
      trader = None
      prefix = None
      
      # Variables para revisar valores duplicados en encabezado y detalle.
      rff_on_header = 0
      rff_aaj_header = 0
      rff_aij_header = 0
      
      # Variable para validar si es la primera línea.
      is_first_LIN = 1
      
      # Variables temporales para almacenar información del documento .EDI
      header_data = []
      detail_data = []
      data = []
      
      # Leer el contenido del documento
      object_with_document = bucket_connection.get_object(Bucket=name_bucket, Key=path_file)
      document_content = object_with_document['Body'].read()
      
      # Bucket donde quedan transformados los EDI en formato parquet
      path_of_processed_file = os.environ['bucket_of_processed_file']+file_name[:-4]+'_L.parquet'
      
      timeZone = dateutil.tz.gettz('America/Bogota')
      format_date = '%Y-%m-%d %H:%M:%S'
      
      # Se asigna fecha y hora de incio de procesamiento de la lambda
      start_execution =  datetime.now(tz=timeZone).strftime(format_date)
      
      try:
  
        # Decodifica el contenido del archivo que tenía formato UTF8
        document_content_decode = document_content.decode('UTF-8')
        
        # Segmenta el documento edit por etiquetas, de esta forma podrás acceder a las variables de una manera fácil
        edi_file = RawSegmentCollection.from_str(document_content_decode)
      
        # Se itera los segmentos del EDI
        for segment in edi_file.segments:
          # This is the data for the headers section from the .EDI document
          # SEGMENT UNB
          if (segment.tag == 'UNB'):
            if len(segment.elements[1]) == 2:
              head_unb_sender = segment.elements[1][0]
            else:
              head_unb_sender = segment.elements[1]
            if len(segment.elements[2]) == 2:  
              head_unb_receiver = segment.elements[2][0]
            else:
              head_unb_receiver = segment.elements[2]
            
          # SEGMENT BGM  
          if (segment.tag == 'BGM'):
            head_bgm_doc_type = segment.elements[0][0]
            head_bgm_recadv_number = segment.elements[1]
          
          # SEGMENT DTM    
          if (segment.tag == 'DTM'):
            if (segment.elements[0][0] == '137'): 
              head_dtm_recadv_date = segment.elements[0][1]
              dtm_receipt_date = head_dtm_recadv_date[8:]
            if (segment.elements[0][0] == '50'): 
              head_dtm_receipt_date = segment.elements[0][1]
              
          # SEGMENT RFF    
          if (segment.tag == 'RFF'):
            if (segment.elements[0][0] == 'ON'):
              if (rff_on_header == 0):
                head_reference_document = segment.elements[0][1]
                rff_on_header = 1
              else:
                det_reference_document = segment.elements[0][1]
                
            if (segment.elements[0][0] == 'FOL'):
              head_folio_number = segment.elements[0][1]
              
            if (segment.elements[0][0] == 'SER'):
              head_serie_number = segment.elements[0][1]
              
            if (segment.elements[0][0] == 'UUID'):
              head_uuid_number = segment.elements[0][1]
                
            if (segment.elements[0][0] == 'IV'):
              head_invoice_number = segment.elements[0][1]
              
            if (segment.elements[0][0] == 'AAU'):
              head_dispatch_number = segment.elements[0][1]
          
          # SEGMENT NAD    
          if (segment.tag == 'NAD'):
            if (segment.elements[0] == 'BY'):
              head_nad_by = segment.elements[1][0]
              # Get Trader name based on head_unb_sender value
              trader = get_name_company(country, head_nad_by, 'C', dynamodb_client)
            if (segment.elements[0] == 'SU'):
              head_nad_su = validate_double_buzon(country, segment.elements[1][0], dynamodb_client)
              # Get Provider name based on head_unb_receiver value
              provider = get_name_company(country, head_nad_su, 'F', dynamodb_client)  
            if (segment.elements[0] == 'DP'):
              head_nad_dp = segment.elements[1][0]
              
          # Data that belongs to header section of recadv    
          header_data = [load_date
                          ,head_unb_sender
                          ,head_unb_receiver
                          ,head_bgm_doc_type
                          ,head_bgm_recadv_number
                          ,head_dtm_recadv_date
                          ,head_dtm_receipt_date
                          ,head_reference_document
                          ,head_invoice_number
                          ,head_dispatch_number
                          ,head_nad_by
                          ,head_nad_su
                          ,head_nad_dp
                          ,head_folio_number
                          ,head_serie_number
                          ,head_uuid_number
                          ,country
                          ,path_of_processed_file]    
              
          # SEGMENT LIN
          if (segment.tag == 'LIN'):
            if (is_first_LIN == 1):
              det_ean_lin = segment.elements[2][0]
              is_first_LIN = 0
            # If not first LIN segment, then create a new line with header + detail  
            else:
              detail_data = header_data + [det_ean_lin
                                            , det_description_lin
                                            , det_sent_quantity
                                            , det_requested_quantity
                                            , det_received_quantity
                                            , det_accepted_quantity
                                            , det_measure_unit
                                            , det_bonus_quantity
                                            , det_toreturn_quantity
                                            , det_todestroy_quantity
                                            , det_pending_quantity
                                            , det_excessive_quantity
                                            , det_damaged_quantity
                                            , det_difference_quantity
                                            , det_total_value
                                            , det_delivery_location
                                            , det_reference_document
                                            , det_toreturn_change_reason]
              # Salto de linea
              data.append(detail_data)
              detail_data = []
              det_ean_lin = segment.elements[2][0]
              det_description_lin = None
              det_sent_quantity = None
              det_requested_quantity = None
              det_received_quantity = None
              det_accepted_quantity = None
              det_measure_unit = None
              det_bonus_quantity = None
              det_toreturn_quantity = None
              det_toreturn_change_reason = None
              det_todestroy_quantity = None
              det_pending_quantity = None
              det_excessive_quantity = None
              det_damaged_quantity = None
              det_difference_quantity = None
              det_total_value = None
              det_delivery_location = None
              det_reference_document = None
              
          # SEGMENT IMD
          if (segment.tag == 'IMD'):
            det_description_lin = segment.elements[2][3]
          
          # SEGMENT QTY
          if (segment.tag == 'QTY'):
            try:
              det_measure_unit = segment.elements[0][2]
            except:
              det_measure_unit = None
            
            if (segment.elements[0][0] == '12'):
              det_sent_quantity = segment.elements[0][1]
            if (segment.elements[0][0] == '21'):
              det_requested_quantity = segment.elements[0][1]
            if (segment.elements[0][0] == '48'):
              det_received_quantity = segment.elements[0][1]  
            if (segment.elements[0][0] == '194'):
              det_accepted_quantity = segment.elements[0][1]
            if (segment.elements[0][0] == '192'):
              det_bonus_quantity = segment.elements[0][1]
          
          # SEGMENT QVR
          if (segment.tag == 'QVR'):
            if (segment.elements[0][1] == '195'):
              det_toreturn_quantity = segment.elements[0][0]
              if (len(segment.elements) == 3):
                det_toreturn_change_reason = segment.elements[2]
              else:
                det_toreturn_change_reason = None
              
            if (segment.elements[0][1] == '196'):
              det_todestroy_quantity = segment.elements[0][0]
            if (segment.elements[0][1] == '73'):
              det_pending_quantity = segment.elements[0][0]
            if (segment.elements[0][1] == '121'):
              det_excessive_quantity = segment.elements[0][0]
            if (segment.elements[0][1] == '124'):
              det_damaged_quantity = segment.elements[0][0]
            if (segment.elements[0][1] == '200'):
              det_difference_quantity = segment.elements[0][0]
          
          # SEGMENT MOA
          if (segment.tag == 'MOA'):
            det_total_value = segment.elements[0][1]
          
          # SEGMENT LOC
          if (segment.tag == 'LOC'):
            if (segment.elements[0] == '7'):
              det_delivery_location = segment.elements[1][0]
              
          # SEGMENT UNZ
          if (segment.tag == 'UNZ'):
            unz_snrf = segment.elements[1]
            detail_data = header_data + [det_ean_lin
                                            , det_description_lin
                                            , det_sent_quantity
                                            , det_requested_quantity
                                            , det_received_quantity
                                            , det_accepted_quantity
                                            , det_measure_unit
                                            , det_bonus_quantity
                                            , det_toreturn_quantity
                                            , det_todestroy_quantity
                                            , det_pending_quantity
                                            , det_excessive_quantity
                                            , det_damaged_quantity
                                            , det_difference_quantity
                                            , det_total_value
                                            , det_delivery_location
                                            , det_reference_document
                                            , det_toreturn_change_reason]
                                      
            # New Line in Data
            data.append(detail_data)
          
        # Iterate to find number of elements in dataframe and to add SNRF field value
        for index, row in enumerate(data):
          total_records += 1
          data[index].append(unz_snrf)
          data[index].append(channel)
          data[index].append(document_type)
        
        # Save EDI Files in backup path
        prefix = 'recadv/'+'country='+country+'/'+'ean_provider='+head_unb_receiver+'/'+'ean_trader='+head_unb_sender+'/'+'year_load='+load_year+'/'+'month_load='+load_month+'/'+'day_load='+load_day
        bucket_connection.put_object(Bucket = os.environ['backup_bucket'], Body = document_content, Key = prefix+'/'+file_name)
        
        # Column DataTypes and Names
        dtypes = {
          'load_date':'string',
          'head_unb_sender':'string',
          'head_unb_receiver':'string',
          'head_bgm_doc_type':'string',
          'head_bgm_recadv_number':'string',
          'head_dtm_recadv_date':'string',
          'head_dtm_receipt_date':'string',
          'head_reference_document':'string',
          'head_invoice_number':'string',
          'head_dispatch_number':'string',
          'head_nad_by':'string',
          'head_nad_su':'string',
          'head_nad_dp':'string',
          'head_folio_number':'string',
          'head_serie_number':'string',
          'head_uuid_number':'string',
          'country':'string',
          'path_of_processed_file':'string',
          'det_ean_lin':'string',
          'det_description_lin':'string',
          'det_sent_quantity':'double',
          'det_requested_quantity':'double',
          'det_received_quantity':'double',
          'det_accepted_quantity':'double',
          'det_measure_unit':'string',
          'det_bonus_quantity':'double',
          'det_toreturn_quantity':'double',
          'det_todestroy_quantity':'double',
          'det_pending_quantity':'double',
          'det_excessive_quantity':'double',
          'det_damaged_quantity':'double',
          'det_difference_quantity':'double',
          'det_total_value':'double',
          'det_delivery_location':'string',
          'det_reference_document':'string',
          'det_toreturn_change_reason':'string',
          'unz_snrf':'string',
          'channel':'string',
          'document_type':'string'
          }
      
        # Generate Parquet file based on dataframe
        wr.s3.to_parquet(dtype=dtypes
          ,df=pd.DataFrame(data
          , columns=['load_date'
          ,'head_unb_sender'
          ,'head_unb_receiver'
          ,'head_bgm_doc_type'
          ,'head_bgm_recadv_number'
          ,'head_dtm_recadv_date'
          ,'head_dtm_receipt_date'
          ,'head_reference_document'
          ,'head_invoice_number'
          ,'head_dispatch_number'
          ,'head_nad_by'
          ,'head_nad_su'
          ,'head_nad_dp'
          ,'head_folio_number'
          ,'head_serie_number'
          ,'head_uuid_number'
          ,'country'
          ,'path_of_processed_file'
          ,'det_ean_lin'
          ,'det_description_lin'
          ,'det_sent_quantity'
          ,'det_requested_quantity'
          ,'det_received_quantity'
          ,'det_accepted_quantity'
          ,'det_measure_unit'
          ,'det_bonus_quantity'
          ,'det_toreturn_quantity'
          ,'det_todestroy_quantity'
          ,'det_pending_quantity'
          ,'det_excessive_quantity'
          ,'det_damaged_quantity'
          ,'det_difference_quantity'
          ,'det_total_value'
          ,'det_delivery_location'
          ,'det_reference_document'
          ,'det_toreturn_change_reason'
          ,'unz_snrf'
          ,'channel'
          ,'document_type']),path=path_of_processed_file)
          
        # DateTime for ending execution
        end_execution = datetime.now(tz=timeZone).strftime(format_date)
          
        # Register bad files in aud_load_audit table from dynamodb
        register_load_audit(country,file_name,head_bgm_recadv_number,unz_snrf,head_unb_receiver,head_unb_sender,load_date,load_day,load_time,load_month,load_year,prefix,provider,trader,dtm_receipt_date,dtm_receipt_date,processing_state,log_audit,start_execution,end_execution,total_records,state,dynamodb_client)  
        
        # remove processed document from bucket
        bucket_connection.delete_object(Bucket = name_bucket, Key = path_file)
        
        return {'statusCode': 200}
        
      except Exception as e:
        
        log_audit = str(e)
        processing_state = 'RECHAZADO POR ESTRUCTURA'
        
        # Register bad files in aud_load_audit table from dynamodb
        register_load_audit(country,file_name,head_bgm_recadv_number,unz_snrf,head_unb_receiver,head_unb_sender,load_date,load_day,load_time,load_month,load_year,prefix,provider,trader,dtm_receipt_date,dtm_receipt_date,processing_state,log_audit,start_execution,end_execution,total_records,state,dynamodb_client)
        
        # Move bad files to rejected bucket
        bucket_connection.put_object(Bucket = os.environ['error_processed_file'], Body = document_content, Key = 'recadv/'+file_name)
        
        # Delete file from raw bucket
        bucket_connection.delete_object(Bucket = name_bucket, Key = path_file)
  except:

      '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
      e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
      catch_error(traceback, context, dateutil, datetime, boto3)
      raise


''' Función que permite obtener el nombre de archivo y extensión '''
def get_filename_extension(my_file_name):
    myfile, myext = os.path.splitext(my_file_name)
    return myfile, myext.lstrip('.')


''' Función que permite guardar auditoria de los EDI recibidos por parte del cliente '''
def register_load_audit(country,file_name,bgm,snrf,ean_provider,ean_trader,load_date,load_day,load_hour,load_month,load_year,path_file,provider,trader,reported_start_date,reported_end_date,processing_state,log_audit,start_execution,end_execution,total_records,state, dynamodb_client):
  
  file_name, extension_file = get_filename_extension(file_name)
  
  dynamodb_client.put_item(
      TableName='aud_load_audit',
      Item={
          'pk': {'S': 'country#' + validate_none_value(country) + '#document_type#' + validate_none_value(document_type)},
          'sk': {'S':  'channel#' + validate_none_value(channel) + '#ean_provider#' + validate_none_value(ean_provider) + '#ean_trader#' + validate_none_value(ean_trader) + '#file_name#' + validate_none_value(file_name)},
          'bgm': {'S': validate_none_value(bgm)},
          'country': {'S': validate_none_value(country)},
          'ean_provider': {'S': validate_none_value(ean_provider)},
          'ean_trader': {'S': validate_none_value(ean_trader)},
          'file_name': {'S': validate_none_value(file_name)},
          'load_date': {'S': validate_none_value(load_date)},
          'load_day': {'S': validate_none_value(load_day)},
          'load_hour': {'S': validate_none_value(load_hour)},
          'load_month': {'S': validate_none_value(load_month)},
          'load_year': {'S': validate_none_value(load_year)},
          'path_file': {'S': os.environ['backup_bucket'] + '/' + validate_none_value(path_file)},
          'provider': {'S': validate_none_value(provider)},
          'reported_end_date': {'S': validate_none_value(reported_end_date)},
          'reported_start_date': {'S': validate_none_value(reported_start_date)},
          'snrf': {'S': validate_none_value(snrf)},
          'state': {'S': state},
          'trader': {'S': validate_none_value(trader)},
          'document_type': {'S': document_type},
          'processing_state': {'S': processing_state},
          'log_audit': {'S': validate_none_value(log_audit)},
          'start_execution': {'S': validate_none_value(start_execution)},
          'end_execution': {'S': validate_none_value(end_execution)},
          'total_records': {'S': str(total_records)},
          'file_name_log': {'S': ''},
          'path_file_log': {'S': ''},
          'path_file_sftp': {'S': ''},
          'channel': {'S': channel},
          'email_load': {'S': 'soporteprescriptiva@gmail.com'},
          'user_load': {'S': 'soporte'},
          'extension_file': {'S': extension_file},
          'details': {'L': []},
          'sk_lsi1': {'S': validate_none_value(file_name)}
      },
      ReturnValues='NONE',
  )   
    

''' Función que permite consultar el nombre de la compañia en dynamodb tabla gen_companies '''
def get_name_company(country, ean_company, type_company, dynamodb_client):

  # Nombre de la tabla y claves
  table_name = 'gen_companies'
  primary_key = {
      'pk': {
          'S': 'company_type#' + type_company
      },
      'sk': {
          'S': 'country#' + country + '#ean_company#' + ean_company
      }
  }

  # Realizar la búsqueda del registro
  response = dynamodb_client.get_item(
      TableName=table_name,
      Key=primary_key
  )
  
  # Si la compañia no existe, entonces seteamos el ean de la compañia como nombre
  if(None == response.get('Item')):
    company = ean_company
  else:
    company = (response.get('Item').get('company_name').get('S'))
  
  return company
  
  
'''
Esta función permite validar si el valor del parámetro "value" tiene el valor "None" y devuelve '' o "value" en caso contrario.
'''
def validate_none_value(value):
    return value if value is not None else ''  
    
    
''' Función que permite validar si el ean del proveedor tiene doble buzon '''
def validate_double_buzon(country, ean_company, dynamodb_client):

  # Nombre de la tabla y claves
  table_name = 'gen_companies_double_buzon'
  primary_key = {
      'pk': {
          'S': 'country#' + country + '#ean_provider_secondary#' + ean_company
      },
      'sk': {
          'S': 'root'
      }
  }

  # Realizar la búsqueda del registro
  response = dynamodb_client.get_item(
      TableName=table_name,
      Key=primary_key
  )
  
  # Si la compañia no existe, entonces seteamos el ean de la compañia como nombre
  if(None != response.get('Item')):
    ean_company = response.get('Item').get('ean_provider_main').get('S')

  return ean_company    
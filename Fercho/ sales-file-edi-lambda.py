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



# Constantes
channel = 'CENT'
document_type = 'sales'


''' Función princial de la lambda '''
def lambda_handler(event, context):
   
  try:

    # allow connection with some bucket
    s3_client = boto3.client('s3')

    # Estableciendo conexión a dynamodb
    dynamodb_client = boto3.client("dynamodb")

    # extract data from document put in bucket. In this case extract the name of bucket and document.
    name_bucket = event['Records'][0]['s3']['bucket']['name']
    
    path_file = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    # The document path is split by the delimiter _
    path_file_split_by_first_delimiter = path_file[:-4].split("_")
    
    path_file_split_by_second_delimiter = path_file.split("/")
    
    # Get the last two values of the array that contains the split path by first q. These two values are document_country and date
    country = path_file_split_by_first_delimiter[len(path_file_split_by_first_delimiter)-1]   # [COUNTRY]
    
    full_date = path_file_split_by_first_delimiter[len(path_file_split_by_first_delimiter)-2]
    
    # Get file name
    file_name = path_file_split_by_second_delimiter[1]
    
    # Split full_date to get the date and time
    load_date = full_date[:8]       # [LOAD_DATE]
    load_time = full_date[8:]       # [LOAD_TIME]
    
    # Split load_date into different variables to get the load_year, load_month and load_day.
    load_year = load_date[:4]
    load_month = load_date[-4:-2]
    load_day = load_date[-2:]
    
    # Temporary variables to store data from .EDI document
    header_data = []
    detail_data = []
    data = []
    lst_duplicate_locs = []
       
    # Temporary Fields referenced by header. Initializing in None
    head_unb_ean_sender = None
    head_unb_ean_receiver = None
    head_unh_msg_type = None
    head_bgm_document = None
    head_nad_ean_sender = None
    head_nad_ean_receiver = None
    head_dtm_report_date = None
    head_unz_sendreference = None
       
    # Temporary Fields referenced by detail. Initializing in None
    det_ean_lin = None
    det_ean_loc = None
    qty = None
    pri_neto = None
    moa = None
    head_dtm_sale_initial_date = None
    head_dtm_sale_final_date = None
    log_audit = None
    processing_state = 'CARGA TOTAL'
    state = 'ACTIVO'
    prefix = None
    provider = None
    trader = None
    total_records = 0
    start_execution = None
    end_execution = None
    is_valid_segments = True
    is_valid_segment_value = True
    is_valid_unt = False
    is_valid_unz = False
    is_first_loc = True
    is_first_lin = True
    
    reload_sales = 'L'
    
    if 'reload_sales' in path_file:
      reload_sales = 'R'
    
    # Leer el contenido del documento
    object_with_document = s3_client.get_object(Bucket=name_bucket, Key=path_file)
    document_content = object_with_document['Body'].read()
    
    # Bucket donde quedan transformados los EDI en formato parquet
    path_of_processed_file = os.environ['bucket_of_processed_file']+file_name[:-4]+'_'+reload_sales+'.parquet'
    
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
          if(segment.tag == 'UNB'):
            segment_value1, is_valid_segment_value1 = validate_segment_value(segment.elements[1])
            segment_value2, is_valid_segment_value2 = validate_segment_value(segment.elements[2])
    
            if(is_valid_segment_value1):
              head_unb_ean_sender = segment_value1       #unb_ean_sender [MANDATORIO]
            else:
              log_audit = 'Valor inválido, validar el identificador del emisor del segmento [UNB]'
              is_valid_segment_value = False
              break
    
            if(is_valid_segment_value2):
              head_unb_ean_receiver = segment_value2     #unb_ean_receiver [MANDATORIO]
            else:
              log_audit = 'Valor inválido, validar el identificador del receptor del segmento [UNB]'
              is_valid_segment_value = False
              break
          
    
          if(segment.tag == 'UNH'):
            segment_value, is_valid_segment_value = validate_segment_value(segment.elements[1][0])
    
            if(is_valid_segment_value):
              head_unh_msg_type = segment_value      #unh_message_type [MANDATORIO]
            else:
              log_audit = 'El valor del segmento [UNH] es obligatorio'
              break
          
    
          # ********** Segment BGM **********
          if(segment.tag == 'BGM'):
            segment_value, is_valid_segment_value = validate_segment_value(segment.elements[1])
    
            if(is_valid_segment_value):
              head_bgm_document = segment_value         #bgm_document [MANDATORIO]
            else:
              log_audit = 'El valor del segmento [BGM] es obligatorio'
              break
    
          
          # ********** Segment NAD **********
          if(segment.tag == 'NAD'):
            isValidQualifier = False
            
            segment_value, is_valid_segment_value = validate_segment_value(segment.elements[1][0])
            
            if(is_valid_segment_value):
            
              if(segment.elements[0] == 'SE'):
                isValidQualifier = True
                head_nad_ean_sender = segment_value          # [EAN_TRADER]
                trader = get_name_company(country, head_nad_ean_sender, 'C', dynamodb_client)
                
              if(segment.elements[0] == 'SU'):
                isValidQualifier = True
                head_nad_ean_receiver = validate_double_buzon(country, segment_value, dynamodb_client)        # [EAN_PROVIDER]
                provider = get_name_company(country, head_nad_ean_receiver, 'F', dynamodb_client)
              
              if(not isValidQualifier):
                log_audit = 'El segmento [NAD] debe contenter los calificadores SE o SU'
                is_valid_segment_value = False
                break
            else:
              log_audit = 'El valor del segmento [NAD] es obligatorio'
              break    
                  
    
          # ********** Segment DTM **********
          if(segment.tag == 'DTM'):            
            isValidQualifier = False
            segment_value, is_valid_segment_value = validate_segment_value(segment.elements[0][1])
            
            if(is_valid_segment_value):
              if (segment.elements[0][0] == '137'):               #dtm_137        
                isValidQualifier = True
                head_dtm_report_date = segment_value
              
              if(segment.elements[0][0] == '356' or segment.elements[0][0] == '90' or segment.elements[0][0] == '91'):                #dtm_356 [SALE_DATE]
              
                if(segment.elements[0][2] == '718'):
                  isValidQualifier = True
                  
                  if(segment.elements[0][0] == '356' or segment.elements[0][0] == '90'):
                    head_dtm_sale_initial_date = segment_value[:8]
                    
                  if(segment.elements[0][0] == '356' or segment.elements[0][0] == '91'):
                    head_dtm_sale_final_date = segment_value[8:]
                
                if(segment.elements[0][2] == '102'):
                  isValidQualifier = True
                  
                  if(segment.elements[0][0] == '356' or segment.elements[0][0] == '90'):
                    head_dtm_sale_initial_date = segment_value[:8]
                    
                  if(segment.elements[0][0] == '356' or segment.elements[0][0] == '91'):   
                    head_dtm_sale_final_date = segment_value[:8]
                  
              if(not isValidQualifier):
                log_audit = 'El segmento [DTM] debe contenter los calificadores 137, 356, 90 o 91'
                is_valid_segment_value = False
                break
            else:
              log_audit = 'El valor del segmento [DTM] es obligatorio'
              break            
              
            detail_data = header_data + [det_ean_lin,qty,det_ean_loc,pri_neto,moa,head_dtm_sale_final_date]
          
          # Lista con valores de encabezado del documento .EDI
          header_data = [load_date,load_time,head_bgm_document,country,head_nad_ean_receiver,head_nad_ean_sender,head_dtm_report_date,path_of_processed_file]
    
    
          # ********** Segment LOC **********
          if(segment.tag == 'LOC'):
            new_ean_loc, is_valid_segment_value = validate_segment_value(segment.elements[1][0])
    
            if(is_valid_segment_value):
              if(is_first_loc):
                is_first_loc = False
              else:
                # Se valida la obligatoriedad de algúnos segmentos para el cambio de linea, el det_ean_loc debe ser el del anterior segmento y no el nuevo que estamos obteniendo
                log_audit = validate_segments(det_ean_lin, det_ean_loc, head_nad_ean_receiver, head_nad_ean_sender, head_dtm_sale_final_date, load_date, load_time, head_bgm_document)
    
                # Se calculan los valores en caso tal de que en el EDI no se hayan enviado
                qty, moa, pri_neto, msj_error_recalculating_values = recalculate_values(qty, moa, pri_neto)
    
                if(log_audit != "" or msj_error_recalculating_values != ""):
                  log_audit += " " + msj_error_recalculating_values + " para el LOC [" + det_ean_loc + "] y LIN [" + det_ean_lin + "]"
                  is_valid_segments = False
                  break
    
                # Se asignan valores recalculados
                detail_data = header_data + [det_ean_lin,qty,det_ean_loc,pri_neto,moa,head_dtm_sale_final_date]
    
                # Salto de linea
                data.append(detail_data)
                detail_data = []
                det_ean_lin = None
                qty = None
                pri_neto = None
                moa = None
                is_first_lin = True
    
              # Asginando el nuevo ean_loc
              det_ean_loc = new_ean_loc
              detail_data = header_data + [det_ean_lin,qty,det_ean_loc,pri_neto,moa,head_dtm_sale_final_date]
            else:
              log_audit = 'El valor del segmento [LOC] es obligatorio para el LOC [' + det_ean_loc + ']'
              is_valid_segment_value = False
              break
          
          
          # ********** Segment LIN **********
          if(segment.tag == 'LIN'):
            isValidQualifier = False
            new_ean_lin, is_valid_segment_value = validate_segment_value(segment.elements[2][0])
    
            if (is_valid_segment_value):
              if(segment.elements[2][1] == 'EN' or segment.elements[2][1] == 'UP'):
                isValidQualifier = True
    
              if(not isValidQualifier):
                log_audit = 'El segmento [LIN] debe contenter los calificadores EN o UP para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
                is_valid_segment_value = False
                break
    
              # Se valida que sea el primer lin del bloque agrupado por loc
              if(is_first_lin):
                is_first_lin = False              
              else:
                # Se valida la obligatoriedad de algúnos segmentos para el cambio de linea, el det_ean_lin debe ser el del anterior segmento y no el nuevo que estamos obteniendo
                log_audit = validate_segments(det_ean_lin, det_ean_loc, head_nad_ean_receiver, head_nad_ean_sender, head_dtm_sale_final_date, load_date, load_time, head_bgm_document)
    
                # Se calculan los valores en caso tal de que en el EDI no se hayan enviado
                qty, moa, pri_neto, msj_error_recalculating_values = recalculate_values(qty, moa, pri_neto)
    
                if(log_audit != "" or msj_error_recalculating_values != ""):
                  log_audit += " " + msj_error_recalculating_values + " para el LOC [" + (det_ean_loc or "") + "] y LIN [" + (det_ean_lin or "") + "]"
                  is_valid_segments = False
                  break
    
                # Se asignan valores recalculados para el segmento anterior
                detail_data = header_data + [det_ean_lin,qty,det_ean_loc,pri_neto,moa,head_dtm_sale_final_date]
    
                # Salto de linea
                data.append(detail_data)
                detail_data = []
                qty = None
                pri_neto = None
                moa = None
    
              # Asginando el nuevo ean_lin
              det_ean_lin = new_ean_lin
              detail_data = header_data + [det_ean_lin,qty,det_ean_loc,pri_neto,moa,head_dtm_sale_final_date]
            else:
              log_audit = 'El valor del segmento [LIN] es obligatorio para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
              is_valid_segment_value = False
              break
    
    
          
          # ********** Segment QTY **********
          if(segment.tag == 'QTY'):
            if(check_number(segment.elements[0][1])):
              val_qty = float(segment.elements[0][1])
            else:
              log_audit = 'Valor inválido del segmento [QTY] para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
              is_valid_segment_value = False
              break
            
            if(segment.elements[0][0] == '153'):
              qty = val_qty
              detail_data = header_data + [det_ean_lin,qty,det_ean_loc,pri_neto,moa,head_dtm_sale_final_date]
            else:
              log_audit = 'El segmento [QTY] debe contenter el calificador 153 para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
              is_valid_segment_value = False
              break
    
    
          # ********** Segment MOA **********                
          if(segment.tag == 'MOA'):              
            if(check_number(segment.elements[0][1].strip())):
              moa = float(segment.elements[0][1])
            else:
              log_audit = 'Valor inválido del segmento [MOA] para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
              is_valid_segment_value = False
              break
            
            detail_data = header_data + [det_ean_lin,qty,det_ean_loc,pri_neto,moa,head_dtm_sale_final_date]
          
    
          # ********** Segment PRI **********    
          if(segment.tag == 'PRI'):
            isValidQualifier = False
            
            if(check_number(segment.elements[0][1])):
              pri_neto = float(segment.elements[0][1])
            else:
              log_audit = 'Valor inválido del segmento [PRI] para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
              is_valid_segment_value = False
              break
            
            if(segment.elements[0][0] == 'AAA' or segment.elements[0][0] == 'AAB'):
              isValidQualifier = True
                    
            if(isValidQualifier):
              detail_data = header_data + [det_ean_lin,qty,det_ean_loc,pri_neto,moa,head_dtm_sale_final_date]
            else:
              log_audit = 'El segmento [PRI] debe contenter los calificadores AAA y/o AAB para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
              is_valid_segment_value = False
              break
    
            
          # ********** Segment UNT **********        
          if(segment.tag == 'UNT'):
            is_valid_unt = True
            
            # Se calculan los valores en caso tal de que en el EDI no se hayan enviado
            qty, moa, pri_neto, msj_error_recalculating_values = recalculate_values(qty, moa, pri_neto)
    
            if (msj_error_recalculating_values != ""):
              log_audit = msj_error_recalculating_values + " para el segmento UNT para el LOC [" + (det_ean_loc or "") + "] y LIN [" + (det_ean_lin or "") + "]"
              is_valid_segments = False
              break
            
            # Se asignan valores recalculados
            detail_data = header_data + [det_ean_lin,qty,det_ean_loc,pri_neto,moa,head_dtm_sale_final_date]
            data.append(detail_data)
    
          # ********** Segment UNZ **********        
          if(segment.tag == 'UNZ'):
            is_valid_unz = True
            head_unz_sendreference = segment.elements[1]
    
      
      # ********** Get the QTY, PRI_AAA or MOA if value is NULL **********
      for index, row in enumerate(data):
        data[index].append(head_unz_sendreference)
        data[index].append(channel)
        data[index].append(document_type)
        
        total_records += 1
          
      # Se valida que todas las filas y segmentos estén correctos.
      if(is_valid_segment_value and is_valid_unt and is_valid_unz and is_valid_segments):
        
        # Guarando los edi en el bucket backup
        prefix = 'sales/'+'country='+country+'/'+'ean_provider='+head_nad_ean_receiver+'/'+'ean_trader='+head_nad_ean_sender+'/'+'year_load='+load_year+'/'+'month_load='+load_month+'/'+'day_load='+load_day        
        s3_client.put_object(Bucket = os.environ['backup_bucket'], Body = document_content, Key = prefix+'/'+file_name)
        
        # Construyendo el PK de la tabla gen_companies de dynamodb
        pk = 'country#' + country
        query = '''select name_company from gen_contracted_products WHERE pk = '{0}' and ean_company = '{1}' and id_option_type = 1 and contains(ean_traders,'{2}')'''.format(pk, head_nad_ean_receiver, head_nad_ean_sender)
        
        # Se ejecuta el query sobre dynamodb
        response_query = dynamodb_client.execute_statement(Statement = query)
        response_query = response_query['Items']
        
        # se valida la relacion comercial entre fabricante y comerciante
        if(len(response_query) > 0):

          # Se define los tipos de columna
          dtypes = {
              'load_date':'string',
              'load_time':'string',
              'bgm_document':'string',
              'country':'string',
              'nad_ean_receiver':'string',
              'nad_ean_sender':'string',
              'dtm_report_date':'string',
              'path_of_processed_file':'string',
              'det_ean_lin':'string',
              'det_qty_sale':'double',
              'det_ean_loc':'string',
              'det_pri_neto':'double',
              'det_pri_neto_total':'double',
              'dtm_sale_date':'string',
              'sendreference':'string',
              'channel':'string',
              'document_type':'string'
          }
    
          # ********** Generate Parquet file based on data list **********
          wr.s3.to_parquet(dtype=dtypes,
                          df=pd.DataFrame(data, columns=['load_date', 'load_time', 'bgm_document', 'country', 'nad_ean_receiver', 'nad_ean_sender', 'dtm_report_date', 'path_of_processed_file', 'det_ean_lin', 'det_qty_sale', 'det_ean_loc', 'det_pri_neto', 'det_pri_neto_total', 'dtm_sale_date', 'sendreference', 'channel', 'document_type']),
                          path=path_of_processed_file)
        else:
          processing_state = 'NO CONTRATADO'
      else:
        processing_state = 'RECHAZADO POR ESTRUCTURA'
        # Se mueve al bucket de rechazados cuando no cumple la estructura o los segmentos obligatorios
        s3_client.put_object(Bucket = os.environ['error_processed_file'], Body = document_content, Key = 'sales/'+file_name)
        
      # Se asigna fecha y hora de fin de procesamiento de la lambda
      end_execution = datetime.now(tz=timeZone).strftime(format_date)
      
      if (reload_sales == 'R'):
        state = 'INACTIVO'
        
      register_load_audit(country,file_name,head_bgm_document,head_unz_sendreference,head_nad_ean_receiver,head_nad_ean_sender,load_date,load_day,load_time,load_month,load_year,prefix,provider,trader,head_dtm_sale_initial_date, head_dtm_sale_final_date,processing_state,log_audit,start_execution,end_execution,total_records,state, dynamodb_client)
     
      # remove processed document from bucket
      s3_client.delete_object(Bucket = name_bucket, Key = path_file)
      
      return {'statusCode': 200}
      
    except Exception as e:
    
      log_audit = str(e)
      processing_state = 'RECHAZADO POR ESTRUCTURA'
      
      # Registrando archivos corruptos en la tabla aud_load_audit de dynamodb
      register_load_audit(country,file_name,head_bgm_document,head_unz_sendreference,head_nad_ean_receiver,head_nad_ean_sender,load_date,load_day,load_time,load_month,load_year,prefix,provider,trader,head_dtm_sale_initial_date, head_dtm_sale_final_date,processing_state,log_audit,start_execution,end_execution,total_records,state, dynamodb_client)
      
      # Movimiento archivos corruptos al bucket de rechazados
      s3_client.put_object(Bucket = os.environ['error_processed_file'], Body = document_content, Key = 'sales/'+file_name)
      
      # Removiendo el archivo EDI procesado del bucket raw
      s3_client.delete_object(Bucket = name_bucket, Key = path_file)
  except:

      '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
      e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
      catch_error(traceback, context, dateutil, datetime, boto3)
      raise



''' Función que permite guardar auditoria de los EDI recibidos por parte del cliente '''
def register_load_audit(country,file_name,bgm,snrf,ean_provider,ean_trader,load_date,load_day,load_hour,load_month,load_year,path_file,provider,trader,reported_start_date,reported_end_date,processing_state,log_audit,start_execution,end_execution,total_records,state, dynamodb_client):
  
  array_file_name = file_name.split(".")
  
  file_name = array_file_name[0]
  extension_file = array_file_name[1]
  
  dynamodb_client.put_item(
      TableName='aud_load_audit',
      Item={
          'pk': {'S': 'country#' + validate_none_value(country) + '#document_type#' + document_type},
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
   


''' Función que permite validar si un valor es numérico '''
def check_number(p_num):
  try:
    float(p_num)
    return True
  except ValueError:
    return False
   


''' Función que permite validar el valor de un segmento '''
def validate_segment_value(segment_value):
  if (segment_value == '' or segment_value is None):
    return segment_value, False
 
  return segment_value, True
 


''' Función que permite validar los segmentos obligatorios '''
def validate_segments(det_ean_lin, det_ean_loc, head_nad_ean_receiver, head_nad_ean_sender, head_dtm_sale_final_date, load_date, load_time, head_bgm_document):

  msj_error = ""

  if(det_ean_loc == None):
    msj_error += 'El segmento [LOC] es obligatorio, '

  if(det_ean_lin == None):
    msj_error += 'El segmento [LIN] es obligatorio, para el LOC [' + det_ean_loc + ']'

  if(head_nad_ean_receiver == None):
    msj_error += 'El segmento [NAD+SU] es obligatorio, '

  if(head_nad_ean_sender == None):
    msj_error += 'El segmento [NAD+BY] es obligatorio, '

  if(head_dtm_sale_final_date == None):
    msj_error += 'El segmento [DTM+206] es obligatorio, '

  if(load_date == None):
    msj_error += 'La fecha de cargue es obligatorio, '

  if(load_time == None):
    msj_error += 'La hora de cargue es obligatorio, '

  if(head_bgm_document == None):
    msj_error += 'El segmento [BGM] es obligatorio'

 
  return msj_error  
       


''' Función que permite calcular los valores en caso tal de que en el EDI no se hayan enviado '''
def recalculate_values(qty, moa, pri_neto):
  msj_error = ""

  if(qty == None):
    if(moa != None and pri_neto != None):
      if(pri_neto == 0):
        if(moa == 0):
          qty = 0
      else:
        qty = (moa / pri_neto)
    else:
      msj_error += 'El valor del [MOA] y [PRIAAA] son obligatorios cuando no se envía el [QTY], '

  if(moa == None):
    if(qty != None and pri_neto != None):
      moa = (qty * pri_neto)
    else:
      msj_error += 'El valor del [QTY] y [PRIAAA] son obligatorios cuando no se envía el [MOA], '

  if(pri_neto == None):
    if(moa != None and qty != None):
      if(qty == 0):
        if(moa == 0):
          pri_neto = 0
      else:
        pri_neto = (moa / qty)
    else:
      msj_error += 'El valor del [MOA] y [QTY] son obligatorios cuando no se envía el [PRIAAA]'

  return qty, moa, pri_neto, msj_error



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



'''
Esta función permite validar si el valor del parámetro "value" tiene el valor "None" y devuelve '' o "value" en caso contrario.
'''
def validate_none_value(value):
    return value if value is not None else ''


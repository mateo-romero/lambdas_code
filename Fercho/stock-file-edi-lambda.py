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
document_type = 'stock'


'''Función princial de la lambda'''
def lambda_handler(event, context):
  
  try:

    # allow connection with some bucket
    s3_client = boto3.client('s3')

    # allow connection with dynamodb 
    dynamodb_client = boto3.client("dynamodb")

    
    # Extract data from document put in bucket. In this case extract the name of bucket and document.
    name_bucket = event['Records'][0]['s3']['bucket']['name'] 
    path_file = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    # The document path is split by the delimiter _
    path_file_split_by_first_delimiter = path_file[:-4].split("_")

    path_file_split_by_second_delimiter = path_file.split("/")

    # Get the last two values of the array that contains the split path by first q. These two values are document_country and date
    country = path_file_split_by_first_delimiter[len(path_file_split_by_first_delimiter)-1]
    full_date = path_file_split_by_first_delimiter[len(path_file_split_by_first_delimiter)-2]

    # Get file name
    file_name = path_file_split_by_second_delimiter[1]

    # Split full_date to get the date and time
    load_date = full_date[:8]
    
    load_time = full_date[8:]
    
    # Split load_date into different variables to get the load_year, load_month and load_day.
    load_year = load_date[:4]
    load_month = load_date[-4:-2]
    load_day = load_date[-2:]
    
    # Temporary Fields referenced by header. Initializing in None
    head_unb_ean_sender = None
    head_unb_ean_receiver = None
    head_unh_msg_type = None
    head_bgm_document = None
    head_nad_ean_sender = None
    head_nad_ean_receiver = None
    head_dtm_report_date = None
    head_dtm_stock_initial_date = None
    head_dtm_stock_final_date = None
    head_unz_sendreference = None
    
    # Temporary Fields referenced by detail. Initializing in None
    det_ean_lin = None
    det_ean_lin_upc = None
    det_pia_in = None
    det_pia_sa = None
    det_imd = None
    det_ali = None
    det_qty_stock = None
    det_qty_in_transit = None
    det_ean_loc = None
    det_pri_neto = None
    det_pri_list = None
    
    # New fields to validate when LOC repeats itself before another LOC
    previous_loc_value = None
    
    # Temporary variables to store data from .EDI document
    header_data = []
    detail_data = []
    data = []
    duplicate_products = []
    duplicate_locations = []
    current_tag = None
    previous_tag = None
    cur_lin_index = 0
    old_lin_index = 0
    lin_tag_counter = 0
    log_audit = None
    processing_state = 'CARGA TOTAL'
    state = 'ACTIVO'
    provider = None
    trader = None
    prefix = None
    total_records = 0
    start_execution = None
    end_execution = None
    
    is_first_loc = True
    temp_qty_145 = None
    temp_qty_198 = None
    
    is_valid_segments = True
    is_valid_segment_value = True
    is_valid_unt = False
    is_valid_unz = False
    
    
    reload_stock = 'L'
    
    if 'reload_stock' in path_file:
      reload_stock = 'R'


    # Leer el contenido del documento
    object_with_document = s3_client.get_object(Bucket=name_bucket, Key=path_file)
    document_content = object_with_document['Body'].read()

    # Bucket donde quedan transformados los EDI en formato parquet
    path_of_processed_file = os.environ['bucket_of_processed_file']+file_name[:-4]+'_'+reload_stock+'.parquet'
    
    timeZone = dateutil.tz.gettz('America/Bogota')
    format_date = '%Y-%m-%d %H:%M:%S'

    # Se asigna fecha y hora de incio de procesamiento de la lambda
    start_execution = datetime.now(tz=timeZone).strftime(format_date)
    
    try:

      # Decodifica el contenido del archivo que tenía formato UTF8
      document_content_decode = document_content.decode('UTF-8')
  
      # Segmenta el documento edit por etiquetas, de esta forma podrás acceder a las variables de una manera fácil
      edi_file = RawSegmentCollection.from_str(document_content_decode)


      # Se itera los segmentos del EDI
      for segment in edi_file.segments:
          
        # These variables are needed to verify rules to detect when there is a new line in the .EDI document
        previous_tag = current_tag
        current_tag = segment.tag
        
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
            

        if(segment.tag == 'BGM'):
          segment_value, is_valid_segment_value = validate_segment_value(segment.elements[1])

          if(is_valid_segment_value):
            head_bgm_document = segment_value         #bgm_document [MANDATORIO]
          else:
            log_audit = 'El valor del segmento [BGM] es obligatorio'
            break
            

        if(segment.tag == 'DTM'):
          isValidQualifier = False
          segment_value, is_valid_segment_value = validate_segment_value(segment.elements[0][1])

          if(is_valid_segment_value):

            if(segment.elements[0][0] == '137'):
              isValidQualifier = True
              head_dtm_report_date = segment_value          #dtm_137 [MANDATORIO]
                
            if(segment.elements[0][0] == '194'):
              isValidQualifier = True
              head_dtm_stock_initial_date = segment_value   #dtm_194 [CONDICIONADO]
                
            if(segment.elements[0][0] == '206'):
              isValidQualifier = True
              head_dtm_stock_final_date = segment_value     #dtm_206 [CONDICIONADO]
              
              # Si el EDI llego sin fecha inicial entonces colocamos fecha final como fecha inicial
              if(None == head_dtm_stock_initial_date):
                head_dtm_stock_initial_date = head_dtm_stock_final_date


            if(not isValidQualifier):
              log_audit = 'El segmento [DTM] debe contenter los calificadores 137, 194 o 206'
              is_valid_segment_value = False
              break
          else:
            log_audit = 'El valor del segmento [DTM] es obligatorio'
            break
        

        if(segment.tag == 'NAD'):
          isValidQualifier = False

          segment_value, is_valid_segment_value = validate_segment_value(segment.elements[1][0])

          if(is_valid_segment_value):
            
            if(segment.elements[0] == 'BY'):
              isValidQualifier = True
              head_nad_ean_sender = segment_value          #nad_ean_sender [MANDATORIO]
              trader = get_name_company(country, head_nad_ean_sender, 'C', dynamodb_client)
                
            if(segment.elements[0] == 'SU'):
              isValidQualifier = True
              head_nad_ean_receiver = validate_double_buzon(country, segment_value, dynamodb_client)        #nad_ean_receiver [MANDATORIO]
              provider = get_name_company(country, head_nad_ean_receiver, 'F', dynamodb_client)


            if(not isValidQualifier):
              log_audit = 'El segmento [NAD] debe contenter los calificadores BY o SU'
              is_valid_segment_value = False
              break
          else:
            log_audit = 'El valor del segmento [NAD] es obligatorio'
            break
            
        
        # Lista con valores de encabezado del documento .EDI
        header_data = [head_bgm_document, head_nad_ean_sender, head_nad_ean_receiver, head_dtm_report_date, head_dtm_stock_initial_date, head_dtm_stock_final_date, country, load_date, load_time]
                        

        # Estos son los datos de la sección de detalles del documento .EDI
        # ********** Segment LIN **********
        if(segment.tag == 'LIN'):
          # Cada vez que llega un segmento LIN, se elimina el contenido de la lista duplicate_locations
          duplicate_locations.clear()
          if (previous_tag == 'LOC'):
            detail_data = header_data + [det_ean_lin,det_ean_lin_upc,det_pia_in,det_pia_sa,det_imd,det_ali,det_qty_stock,det_qty_in_transit,det_ean_loc,det_pri_neto,det_pri_list,path_of_processed_file]
              
          isValidQualifier = False
          segment_value, is_valid_segment_value = validate_segment_value(segment.elements[2][0])

          if(is_valid_segment_value): 
            lin_tag_counter += 1

            if(segment.elements[2][1] == 'EN' or segment.elements[2][1] == 'UP'):
              det_ean_lin = segment_value
              det_ean_lin_upc = segment_value
              isValidQualifier = True

            if(not isValidQualifier):
              log_audit = 'El segmento [LIN] debe contenter los calificadores EN o UP para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
              is_valid_segment_value = False
              break

            # Se valida si el lin (ean product) esta duplicado dento del EDI
            if(det_ean_lin in duplicate_products):
              log_audit = "EAN de LIN duplicado [" + det_ean_lin + "]"
              is_valid_segments = False
              break
            else:
              duplicate_products.append(det_ean_lin)
            
            if (lin_tag_counter == 1):
              old_lin_index = segment.elements[0]
            else:
              old_lin_index = cur_lin_index
                
            cur_lin_index = segment.elements[0]
            
            # Se valida que se un salto de linea
            if(is_new_line(old_lin_index, cur_lin_index, current_tag)):
              
              # Se valida la obligatoriedad de algúnos segmentos
              log_audit = validate_segments(det_ean_lin, det_ean_loc, det_qty_stock, head_nad_ean_receiver, head_nad_ean_sender, head_dtm_stock_final_date, load_date, load_time, head_bgm_document)

              if(log_audit != ""):
                is_valid_segments = False
                break

              data.append(detail_data)
              detail_data = []
              det_pia_in = None 
              det_pia_sa = None
              det_imd = None
              det_ali = None
              det_ean_loc = None
              det_qty_stock = None
              det_qty_in_transit = None
              det_pri_neto = None
              det_pri_list = None
              is_first_loc = True
            else:
              detail_data = header_data + [det_ean_lin,det_ean_lin_upc,det_pia_in,det_pia_sa,det_imd,det_ali,det_qty_stock,det_qty_in_transit,det_ean_loc,det_pri_neto,det_pri_list,path_of_processed_file]  
          else:
            log_audit = 'El valor del segmento [DET_LIN_EAN] o [DET_LIN_UPC] es obligatorio para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
            is_valid_segment_value = False
            break


        # ********** Segment PIA **********
        if(segment.tag == 'PIA'): 

          isValidQualifier = False
          segment_value, is_valid_segment_value = validate_segment_value(segment.elements[1][0])

          if(segment.elements[1][1] == 'IN'):
            det_pia_in = segment_value
            isValidQualifier = True

          if(segment.elements[1][1] == 'SA'):
            det_pia_sa = segment_value
            isValidQualifier = True

          if(is_valid_segment_value and isValidQualifier):
            detail_data = header_data + [det_ean_lin,det_ean_lin_upc,det_pia_in,det_pia_sa,det_imd,det_ali,det_qty_stock,det_qty_in_transit,det_ean_loc,det_pri_neto,det_pri_list,path_of_processed_file]
          else:
            log_audit = 'Valor inválido, validar el segmento [PIA] y/o sus calificadores para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
            is_valid_segment_value = False
            break
        

        # ********** Segment IMD **********
        if(segment.tag == 'IMD'):
          det_imd = segment.elements[2][3]
          detail_data = header_data + [det_ean_lin,det_ean_lin_upc,det_pia_in,det_pia_sa,det_imd,det_ali,det_qty_stock,det_qty_in_transit,det_ean_loc,det_pri_neto,det_pri_list,path_of_processed_file]


        # ********** Segment ALI **********
        if(segment.tag == 'ALI'):
          det_ali = segment.elements[0]
          detail_data = header_data + [det_ean_lin,det_ean_lin_upc,det_pia_in,det_pia_sa,det_imd,det_ali,det_qty_stock,det_qty_in_transit,det_ean_loc,det_pri_neto,det_pri_list,path_of_processed_file]
            

        # ********** Segment QTY **********
        if(segment.tag == 'QTY'):

          isValidQualifier = False

          if(check_number(segment.elements[0][1])):
            val_qty = float(segment.elements[0][1])
          else:
            log_audit = 'Valor inválido del segmento [QTY] para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
            is_valid_segment_value = False
            break
              

          if(segment.elements[0][0] == '145'):
            isValidQualifier = True
            if(det_qty_stock == None):
              det_qty_stock = val_qty
            else:
              temp_qty_145 = val_qty
                  
          if(segment.elements[0][0] == '198'):
            isValidQualifier = True
            if(det_qty_in_transit == None):
              det_qty_in_transit = val_qty
            else:
              temp_qty_198 = val_qty

          if(isValidQualifier):
            detail_data = header_data + [det_ean_lin,det_ean_lin_upc,det_pia_in,det_pia_sa,det_imd,det_ali,det_qty_stock,det_qty_in_transit,det_ean_loc,det_pri_neto,det_pri_list,path_of_processed_file]
          else:
            log_audit = 'El segmento [QTY] debe contenter los calificadores 145 y/o 198 para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
            is_valid_segment_value = False
            break
                  
        
        # ********** Segment LOC **********        
        if(segment.tag == 'LOC'):
          segment_value, is_valid_segment_value = validate_segment_value(segment.elements[1][0])

          if(is_valid_segment_value):
            det_ean_loc = segment_value
            
            # Se valida si el loc (ean point sale) esta duplicado dentro del LIN del EDI. Si está duplicado, se finaliza el ciclo y se rechaza el archivo
            if(det_ean_loc in duplicate_locations):
              log_audit = "EAN de LOC duplicado [" + det_ean_loc + "] en el LIN ["+ det_ean_lin + "]"
              is_valid_segments = False
              break
            else:
              duplicate_locations.append(det_ean_loc)

            if(is_first_loc):
              previous_loc_value = det_ean_loc
              detail_data = header_data + [det_ean_lin,det_ean_lin_upc,det_pia_in,det_pia_sa,det_imd,det_ali,det_qty_stock,det_qty_in_transit,det_ean_loc,det_pri_neto,det_pri_list,path_of_processed_file]
              is_first_loc = False
            else:
              if(det_ean_loc != previous_loc_value):
                data.append(detail_data)
                detail_data = []
                det_ean_loc = segment.elements[1][0]
                det_pri_neto = None
                det_pri_list = None
                previous_loc_value = segment.elements[1][0]
                det_qty_stock = temp_qty_145
                det_qty_in_transit = temp_qty_198
                temp_qty_145 = None
                temp_qty_198 = None
          else:
            log_audit = 'El valor del segmento [LOC] es obligatorio para el LOC [' + det_ean_loc + ']'
            is_valid_segment_value = False
            break


        # ********** Segment PRI **********    
        if(segment.tag == 'PRI'):
          isValidQualifier = False

          if(check_number(segment.elements[0][1])):
            val_pri = float(segment.elements[0][1])
          else:
            log_audit = 'Valor inválido del segmento [PRI] para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
            is_valid_segment_value = False
            break
          
          if(segment.elements[0][0] == 'AAA'):
            isValidQualifier = True
            if(det_pri_neto == None):
              det_pri_neto = val_pri
                  
          if(segment.elements[0][0] == 'AAB'):
            isValidQualifier = True
            if(det_pri_list == None):
              det_pri_list = val_pri
                  
        
          if(isValidQualifier):
            detail_data = header_data + [det_ean_lin,det_ean_lin_upc,det_pia_in,det_pia_sa,det_imd,det_ali,det_qty_stock,det_qty_in_transit,det_ean_loc,det_pri_neto,det_pri_list,path_of_processed_file]
          else:
            log_audit = 'El segmento [PRI] debe contenter los calificadores AAA y/o AAB para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'
            is_valid_segment_value = False
            break


        # ********** Segment UNT **********        
        if(segment.tag == 'UNT'):
          is_valid_unt = True
          # Modificación nuevo detail_data
          detail_data = header_data + [det_ean_lin, det_ean_lin_upc, det_pia_in, det_pia_sa, det_imd, det_ali, det_qty_stock, det_qty_in_transit, det_ean_loc, det_pri_neto, det_pri_list, path_of_processed_file]
          data.append(detail_data)

          if det_ean_lin is None or det_ean_lin is None:
            log_audit = 'El segmento [LIN] es obligatorio'
            is_valid_segment_value = False
            break

          if det_ean_loc is None:
            log_audit = 'El segmento [LOC] es obligatorio'
            is_valid_segment_value = False
            break

          if det_qty_stock is None:
            log_audit = 'El segmento [QTY+145] es obligatorio'
            is_valid_segment_value = False
            break


        # ********** Segment UNT **********        
        if(segment.tag == 'UNZ'):
          is_valid_unz = True
          head_unz_sendreference = segment.elements[1] 

      for index,row in enumerate(data):
        data[index].append(head_unz_sendreference)
        data[index].append(channel)
        data[index].append(document_type)
        
        total_records += 1
      
      
      # Se valida que todas las filas y segmentos estén correctos.
      if(is_valid_segment_value and is_valid_unt and is_valid_unz and is_valid_segments):
        
        # Si la estructura del EDI es correcta se guarda un backup
        prefix = 'stock/'+'country='+country+'/'+'ean_provider='+head_nad_ean_receiver+'/'+'ean_trader='+head_nad_ean_sender+'/'+'year_load='+load_year+'/'+'month_load='+load_month+'/'+'day_load='+load_day
        s3_client.put_object(Bucket = os.environ['backup_bucket'], Body = document_content, Key = prefix+'/'+file_name)
       
        # Construyendo el PK de la tabla gen_companies de dynamodb
        pk = 'country#' + country
        query = '''select name_company from gen_contracted_products WHERE pk = '{0}' and ean_company = '{1}' and id_option_type = 1 and contains(ean_traders,'{2}')'''.format(pk, head_nad_ean_receiver, head_nad_ean_sender)
   
        # Ejecutando el query en dynamodb
        response_query = dynamodb_client.execute_statement(Statement = query)
        response_query = response_query['Items']

        # Se valida que el fabricante y el comerciante tengan el producto contratado "ventas e inventarios" para parquetizar el EDI
        if(len(response_query) > 0):
          
          # Se define los tipos de columna
          dtypes = {
            'bgm_document':'string', 
            'nad_ean_sender':'string', 
            'nad_ean_receiver':'string', 
            'dtm_report_date':'string', 
            'dtm_stock_initial_date':'string', 
            'dtm_stock_final_date':'string', 
            'country':'string', 
            'load_date':'string', 
            'load_time':'string', 
            'det_ean_lin':'string', 
            'det_ean_lin_upc':'string', 
            'det_pia_in':'string', 
            'det_pia_sa':'string', 
            'det_imd':'string', 
            'det_ali':'string', 
            'det_qty_stock':'double', 
            'det_qty_in_transit':'double', 
            'det_ean_loc':'string', 
            'det_pri_neto':'double', 
            'det_pri_list':'double',
            'path_of_processed_file':'string',
            'sendreference':'string',
            'channel':'string',
            'document_type':'string'
          }
          
          # ********** Generando archivo parquet ********** 
          wr.s3.to_parquet(dtype=dtypes, 
                          df=pd.DataFrame(data, columns=['bgm_document', 'nad_ean_sender', 'nad_ean_receiver', 'dtm_report_date', 'dtm_stock_initial_date', 'dtm_stock_final_date', 'country', 'load_date', 'load_time', 'det_ean_lin', 'det_ean_lin_upc', 'det_pia_in', 'det_pia_sa', 'det_imd', 'det_ali', 'det_qty_stock', 'det_qty_in_transit', 'det_ean_loc', 'det_pri_neto', 'det_pri_list','path_of_processed_file','sendreference', 'channel', 'document_type']),
                          path=path_of_processed_file)
        else:
          processing_state = 'NO CONTRATADO'
      else:
          processing_state = 'RECHAZADO POR ESTRUCTURA'
          # Se mueve al bucket de rechazados cuando no cumple la estructura o los segmentos obligatorios
          s3_client.put_object(Bucket = os.environ['error_processed_file'], Body = document_content, Key = 'stock/'+file_name)


      # Se asigna fecha y hora de fin de procesamiento de la lambda
      end_execution = datetime.now(tz=timeZone).strftime(format_date)
      
      if reload_stock == 'R':   
        state = 'INACTIVO'
      
      register_load_audit(country,file_name,head_bgm_document,head_unz_sendreference,head_nad_ean_receiver,head_nad_ean_sender,load_date,load_day,load_time,load_month,load_year,prefix,provider,trader,head_dtm_stock_initial_date,head_dtm_stock_final_date,processing_state,log_audit,start_execution,end_execution,total_records,state,dynamodb_client)
          
      # Removiendo el archivo EDI procesado del bucket raw
      s3_client.delete_object(Bucket = name_bucket, Key = path_file)
      
      return {'statusCode': 200}
      
    except Exception as e:  

      log_audit = str(e)
      processing_state = 'RECHAZADO POR ESTRUCTURA'


      # Registrando archivos corruptos en la tabla aud_load_audit de dynamodb
      register_load_audit(country,file_name,head_bgm_document,head_unz_sendreference,head_nad_ean_receiver,head_nad_ean_sender,load_date,load_day,load_time,load_month,load_year,prefix,provider,trader,head_dtm_stock_initial_date,head_dtm_stock_final_date,processing_state,str(e),start_execution,end_execution,total_records,state,dynamodb_client)
      
      # Movimiento archivos corruptos al bucket de rechazados
      s3_client.put_object(Bucket = os.environ['error_processed_file'], Body = document_content, Key = 'stock/'+file_name)
      
      # Removiendo el archivo EDI procesado del bucket raw
      s3_client.delete_object(Bucket = name_bucket, Key = path_file)
  except:

    '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
    e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
    catch_error(traceback, context, dateutil, datetime, boto3)
    raise


'''Función que permite guardar auditoria de los EDI recibidos por parte del cliente'''
def register_load_audit(country,file_name,bgm,snrf,ean_provider,ean_trader,load_date,load_day,load_hour,load_month,load_year,path_file,provider,trader,reported_start_date,reported_end_date,processing_state,log_audit,start_execution,end_execution,total_records,state,dynamodb_client):
  
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



'''Función que permite validar si un valor es numérico'''
def check_number(p_num):
  try:
    float(p_num)
    return True
  except ValueError:
    return False


'''Función que permite validar el valor de un segmento'''
def validate_segment_value(segment_value):
  if (segment_value == '' or segment_value is None):
    return segment_value, False
  
  return segment_value, True


'''Función que permite validar los segmentos obligatorios'''
def validate_segments(det_ean_lin, det_ean_loc, det_qty_stock, head_nad_ean_receiver, head_nad_ean_sender, head_dtm_stock_final_date, load_date, load_time, head_bgm_document):

  msj_error = ""

  if(det_ean_loc == None):
    msj_error += 'El segmento [LOC] es obligatorio, '

  if(det_ean_lin == None):
    msj_error += 'El segmento [LIN] es obligatorio, para el LOC [' + det_ean_loc + ']'

  if(det_qty_stock == None):
    msj_error += 'El segmento [QTY] es obligatorio, para el LOC [' + det_ean_loc + '] y LIN [' + det_ean_lin + ']'

  if(head_nad_ean_receiver == None):
    msj_error += 'El segmento [NAD+SU] es obligatorio, '

  if(head_nad_ean_sender == None):
    msj_error += 'El segmento [NAD+BY] es obligatorio, '

  if(head_dtm_stock_final_date == None):
    msj_error += 'El segmento [DTM+206] es obligatorio, '

  if(load_date == None):
    msj_error += 'La fecha de cargue es obligatorio, '

  if(load_time == None):
    msj_error += 'La hora de cargue es obligatorio, '

  if(head_bgm_document == None):
    msj_error += 'El segmento [BGM] es obligatorio'
  
  return msj_error
  
 
'''Función que permite validar el valor el cambio de linea en el EDI'''
def is_new_line(p_old_lin_index, p_cur_lin_index, p_current_tag):

  if((p_current_tag == 'LIN' and p_old_lin_index != p_cur_lin_index) or (p_current_tag == 'UNT')):
    return True
  else:
    return False


'''Función que permite consultar el nombre de la compañia en dynamodb tabla gen_companies'''
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


'''Función que permite validar si el ean del proveedor tiene doble buzon'''
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


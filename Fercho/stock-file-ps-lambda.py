import boto3
import pandas as pd
from datetime import datetime
import dateutil
import urllib.parse
import awswrangler as wr
import os
import traceback
from catch_error import catch_error



# Constantes con mensajes de error para log_audit
error_index = 'Error en la línea {0}: No se encontró valor del campo en la posición {1}\r\n'
error_element_numbers = 'Error en la línea {0}: Se esperaban al menos {1} elementos de carácter obligatorio.\r\n'
error_value = 'Error en la línea {0}: Se esperaba un número en la posición {1}\r\n'
error_empty_line = 'Error en la línea {0}: La línea no contiene elementos.\r\n'

# Constantes
channel = 'CENT'
document_type = 'stock_ps'


''' Función principal '''
def lambda_handler(event, context):
    
    try:
    
        # allow connection with some bucket
        s3_client = boto3.client('s3')
    
        # Estableciendo conexión a dynamodb
        dynamodb_client = boto3.client("dynamodb")
        
        # Extraer información del documento que llegó al bucket. En este caso extrae el nombre del bucket.
        name_bucket = event['Records'][0]['s3']['bucket']['name']
        
        path_file = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        
        _file_name, _file_extension = os.path.splitext(path_file)
        _num_chars = -1*len(_file_extension)
        
        # Se hace split por el separador "_"
        path_file_split_by_first_delimiter = path_file[:_num_chars].split("_")
        
        # Se hace split por el separador "/"
        path_file_split_by_second_delimiter = path_file.split("/")
        
        # Get file name
        file_name = path_file_split_by_second_delimiter[len(path_file_split_by_second_delimiter)-1]
        
        # Se obtiene el país a partir del nombre del archvio.
        country = path_file_split_by_first_delimiter[len(path_file_split_by_first_delimiter)-1]
        
        current_load_date = datetime.now()
    
        load_date = current_load_date.strftime("%Y%m%d")
        load_time = current_load_date.strftime("%H%M%S")
        load_year = load_date[:4]
        load_month = load_date[-4:-2]
        load_day = load_date[-2:]
        
        object_with_document = s3_client.get_object(Bucket=name_bucket, Key=path_file)
        document_content = object_with_document['Body'].read()
        
        # Bucket donde quedan transformados los EDI en formato parquet
        path_of_processed_file = os.environ['bucket_of_processed_file']+file_name[:_num_chars]+'.parquet'
        
        timeZone = dateutil.tz.gettz('America/Bogota')
        format_date = '%Y-%m-%d %H:%M:%S'
        
        # Se asigna fecha y hora de incio de procesamiento de la lambda
        start_execution =  datetime.now(tz=timeZone).strftime(format_date)
        
        line_counter = 0
        processing_state = 'CARGA TOTAL'
        state = 'ACTIVO'
        prefix = ''
        
        #Variables para los segmentos de la línea 1
        UNB_01 = ''
        UNB_01_AUTOMATIZATION = ''
        EAN_SENDER = ''
        EAN_RECEIVER = ''
        DOCUMENT_TYPE = ''
        AUTOMATIZATION_NUMBER = ''
        CONSECUTIVE_NUMBER = ''
        
        #Variables para los segmentos de la línea 2
        UNB_02 = ''
        UNB_02_AUTOMATIZATION = ''
        STOCKS_REPORT_NUMBER = ''
        INVRPT_INI_DATE = ''
        INVRPT_END_DATE = ''
        INVRPT_REPORT_DATE = ''
        EAN_TRADER = ''
        EAN_PROVIDER = ''
        TMP_EAN_PROVIDER = ''
        
        #Variables para los campos del detalle
        EAN_PRODUCT = ''                  #1  M
        EAN_PRODUCT_UPC = ''              #1  M
        EAN_TYPE = ''                     #2  M  
        EAN_POINT_SALE = ''               #3  M
        STOCK_QUANTITY = None             #4  M
        STOCK_QUANTITY_IN_TRANSIT = None  #5  O   
        NET_PRICE = None                  #16 O
        DET_IMD = ''                      #17 O
        DET_PIA_IN = ''                   #18 O
        DET_PIA_SA = ''                   #19 O
        DET_ALI = ''                      #20 O
        LIST_PRICE = None                 #32 O
    
        # Variables para almacenar el archivo parseado
        detail_data = []
        data = []
        
        # Variables para almacenar los mensajes de error
        log_audit = ''
        tmp_log_audit = ''
        
        # Dividir la cadena en líneas
        csv_reader = document_content.decode('utf-8').split('\r\n')
        
        # Omitir la última línea si está vacía.
        # Se hace esta validación porque muchos archivos vienen con un salto de línea adicional
        if csv_reader and csv_reader[-1] == "":
            csv_reader.pop()
        
        try:
        
            for linea in csv_reader:
                line_complete = False
        
                line_counter += 1
                
                if line_counter in (1, 2):
                        
                    separator = '+'
                    field_list = linea.split(separator)
                    
                    if line_counter == 1:
                        elements = len(field_list)
                        if elements > 0:
                            line_complete = True
                        if elements >= 7:
    
                            UNB_01, tmp_log_audit = field_value_validation(field_list[0].strip(), line_counter, 1, error_index, False)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit 
    
                            UNB_01_AUTOMATIZATION, tmp_log_audit = field_value_validation(field_list[1].strip(), line_counter, 2, error_index, False)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
    
                            EAN_SENDER, tmp_log_audit = field_value_validation(field_list[2].strip(), line_counter, 3, error_index, False)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
    
                            EAN_RECEIVER, tmp_log_audit = field_value_validation(field_list[3].strip(), line_counter, 4, error_index, False)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
    
                            DOCUMENT_TYPE, tmp_log_audit = field_value_validation(field_list[4].strip(), line_counter, 5, error_index, False)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
    
                            AUTOMATIZATION_NUMBER, tmp_log_audit = field_value_validation(field_list[5].strip(), line_counter, 6, error_index, True)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
    
                            CONSECUTIVE_NUMBER, tmp_log_audit = field_value_validation(field_list[6].strip(), line_counter, 7, error_index, False)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
                        else:
                            log_audit += error_element_numbers.format(line_counter, 6)
                            break
                    
                    elif line_counter == 2:
    
                        elements = len(field_list)
    
                        if elements >= 6:
                            line_complete = True
    
                            UNB_02, tmp_log_audit = field_value_validation(field_list[0].strip(), line_counter, 1, error_index, False)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
    
                            UNB_02_AUTOMATIZATION, tmp_log_audit = field_value_validation(field_list[1].strip(), line_counter, 2, error_index, False)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
    
                            STOCKS_REPORT_NUMBER, tmp_log_audit = field_value_validation(field_list[2].strip(), line_counter, 3, error_index, False)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
    
                            INVRPT_INI_DATE, tmp_log_audit = field_value_validation(field_list[3].strip(), line_counter, 4, error_index, True)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
    
                            INVRPT_END_DATE, tmp_log_audit = field_value_validation(field_list[4].strip(), line_counter, 5, error_index, True)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
    
                            INVRPT_REPORT_DATE, tmp_log_audit = field_value_validation(field_list[5].strip(), line_counter, 6, error_index, True)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
    
                            try:
                                EAN_TRADER = field_list[6]
                            except IndexError:
                                EAN_TRADER = ''
        
                            try:
                                EAN_PROVIDER = field_list[7]
                            except IndexError:
                                EAN_PROVIDER = ''
        
                            if EAN_TRADER == '' or EAN_TRADER == EAN_SENDER:
                                EAN_TRADER = EAN_SENDER
                                
                            if EAN_PROVIDER == '' or EAN_PROVIDER == EAN_RECEIVER:
                                TMP_EAN_PROVIDER = EAN_RECEIVER
                                
                            EAN_PROVIDER = validate_double_buzon(country, TMP_EAN_PROVIDER, dynamodb_client)
                                
                            trader = get_name_company(country, EAN_TRADER, 'C', dynamodb_client)
                            provider = get_name_company(country, EAN_PROVIDER, 'F', dynamodb_client)
                        else:
                            log_audit += error_element_numbers.format(line_counter, 5)
                            break
    
                else:
                    separator = ','
        
                    elements = len(linea.split(separator))
        
                    if elements >= 5:
        
                        line_complete = True
                        
                        EAN_TYPE, tmp_log_audit = field_value_validation(linea.split(separator)[1].strip(), line_counter, 2, error_index, False)
                        if tmp_log_audit != '':
                            log_audit += tmp_log_audit   
                            
                        if EAN_TYPE == 'UP':
                            EAN_PRODUCT_UPC, tmp_log_audit = field_value_validation(linea.split(separator)[0].strip(), line_counter, 1, error_index, False)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
                                
                            EAN_PRODUCT, tmp_log_audit = field_value_validation(linea.split(separator)[0].strip(), line_counter, 1, error_index, False)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit    
                        elif EAN_TYPE == 'EN':
                            EAN_PRODUCT, tmp_log_audit = field_value_validation(linea.split(separator)[0].strip(), line_counter, 1, error_index, False)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
                                
                            EAN_PRODUCT_UPC, tmp_log_audit = field_value_validation(linea.split(separator)[0].strip(), line_counter, 1, error_index, False)
                            if tmp_log_audit != '':
                                log_audit += tmp_log_audit
                                
                        EAN_POINT_SALE, tmp_log_audit = field_value_validation(linea.split(separator)[2].strip(), line_counter, 3, error_index, False)
                        if tmp_log_audit != '':
                            log_audit += tmp_log_audit
                            
                        STOCK_QUANTITY, tmp_log_audit = field_value_validation(linea.split(separator)[3].strip(), line_counter, 4, error_index, True)
                        if tmp_log_audit != '':
                            log_audit += tmp_log_audit
                            
                        if (linea.split(separator)[4] != ''):
                            STOCK_QUANTITY_IN_TRANSIT, tmp_log_audit = field_value_validation(linea.split(separator)[4].strip(), line_counter, 5, error_index, True)
                        else:
                            STOCK_QUANTITY_IN_TRANSIT = None
                        if tmp_log_audit != '':
                            log_audit += tmp_log_audit
                            
                        if (linea.split(separator)[15] != ''):
                            NET_PRICE, tmp_log_audit = field_value_validation(linea.split(separator)[15].strip(), line_counter, 16, error_index, True)
                        else:
                            NET_PRICE = None
                        if tmp_log_audit != '':
                            log_audit += tmp_log_audit
                            
                        try:
                            DET_IMD = linea.split(separator)[16]
                        except IndexError:
                            DET_IMD = ''
                            
                        try:
                            DET_PIA_IN = linea.split(separator)[17]
                        except IndexError:
                            DET_PIA_IN = ''
                        
                        try:
                            DET_PIA_SA = linea.split(separator)[18]
                        except IndexError:
                            DET_PIA_SA = ''
                        
                        try:
                            DET_ALI = linea.split(separator)[19]
                        except IndexError:
                            DET_ALI = ''
                        
                        try:
                            LIST_PRICE = float(linea.split(separator)[31])
                        except IndexError:
                            LIST_PRICE = None
                        except ValueError:
                            log_audit += error_value.format(line_counter, 32)
        
                        detail_data = [CONSECUTIVE_NUMBER,  EAN_SENDER, EAN_RECEIVER, INVRPT_REPORT_DATE, INVRPT_INI_DATE, INVRPT_END_DATE, country
                                        , load_date, load_time, EAN_PRODUCT, EAN_PRODUCT_UPC, DET_PIA_IN, DET_PIA_SA, DET_IMD, DET_ALI, STOCK_QUANTITY
                                        , STOCK_QUANTITY_IN_TRANSIT, EAN_POINT_SALE, NET_PRICE, LIST_PRICE, path_of_processed_file, CONSECUTIVE_NUMBER, channel, document_type]
        
                        data.append(detail_data)
                        
                    else:
                        log_audit += error_element_numbers.format(line_counter, 4, elements)
                        break
            
                    if log_audit != '':
                        prefix = os.environ['error_processed_file']+'/stock_ps/'+file_name
                        break
            
            # Conteo de registros generados
            total_records = 0
            for row in enumerate(data):
                total_records += 1
                
            # Se valida que todas las filas y segmentos estén correctos.
            if(log_audit == ''):
                # Guardando los edi en el bucket backup
                prefix = 'stock_ps/'+'country='+country+'/'+'ean_provider='+EAN_RECEIVER+'/'+'ean_trader='+EAN_SENDER+'/'+'year_load='+load_year+'/'+'month_load='+load_month+'/'+'day_load='+load_day        
                s3_client.put_object(Bucket = os.environ['backup_bucket'], Body = document_content, Key = prefix+'/'+file_name)
                prefix = os.environ['backup_bucket']+'/'+prefix+'/'+file_name
                
                # Construyendo el PK de la tabla gen_companies de dynamodb
                pk = 'country#' + country
                query = '''select name_company from gen_contracted_products WHERE pk = '{0}' and ean_company = '{1}' and id_option_type = 1 and contains(ean_traders,'{2}')'''.format(pk, EAN_PROVIDER, EAN_TRADER)
                
                # Se ejecuta el query sobre dynamodb
                response_query = dynamodb_client.execute_statement(Statement = query)
                response_query = response_query['Items']
                
                # se valida la relacion comercial entre fabricante y comerciante
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
                
                    # ********** Generate Parquet file based on data list **********
                    wr.s3.to_parquet(dtype=dtypes,
                        df=pd.DataFrame(data, columns=['bgm_document', 'nad_ean_sender', 'nad_ean_receiver', 'dtm_report_date', 'dtm_stock_initial_date'
                        , 'dtm_stock_final_date', 'country', 'load_date', 'load_time', 'det_ean_lin', 'det_ean_lin_upc', 'det_pia_in', 'det_pia_sa'
                        , 'det_imd', 'det_ali','det_qty_stock','det_qty_in_transit','det_ean_loc','det_pri_neto','det_pri_list','path_of_processed_file','sendreference', 'channel', 'document_type']),
                        path=path_of_processed_file)
                else:
                    processing_state = 'NO CONTRATADO'
            else:
                processing_state = 'RECHAZADO POR ESTRUCTURA'
                # Se mueve al bucket de rechazados cuando no cumple la estructura o los segmentos obligatorios
                s3_client.put_object(Bucket = os.environ['error_processed_file'], Body = document_content, Key = 'stock_ps/'+file_name)
                prefix = os.environ['error_processed_file']+'/stock_ps/'+file_name
            
            # Se asigna fecha y hora de fin de procesamiento de la lambda
            end_execution = datetime.now(tz=timeZone).strftime(format_date)
            register_load_audit(country,file_name,CONSECUTIVE_NUMBER,CONSECUTIVE_NUMBER,EAN_PROVIDER,EAN_TRADER,load_date,load_day,load_time,load_month,load_year,prefix,provider,trader,INVRPT_INI_DATE, INVRPT_END_DATE,processing_state,log_audit,start_execution,end_execution,total_records,state, dynamodb_client)
         
            # remove processed document from bucket
            s3_client.delete_object(Bucket = name_bucket, Key = path_file)
              
            return {'statusCode': 200}
          
        except Exception as e:
            # Se asigna fecha y hora de fin de procesamiento de la lambda
            end_execution = datetime.now(tz=timeZone).strftime(format_date)
            log_audit = str(e)
            processing_state = 'RECHAZADO POR ESTRUCTURA'
            
            prefix = os.environ['error_processed_file']+'/stock_ps/'+file_name
              
            # Registrando archivos corruptos en la tabla aud_load_audit de dynamodb
            register_load_audit(country,file_name,CONSECUTIVE_NUMBER,CONSECUTIVE_NUMBER,EAN_PROVIDER,EAN_TRADER,load_date,load_day,load_time,load_month,load_year,prefix,provider,trader,INVRPT_INI_DATE, INVRPT_END_DATE,processing_state,log_audit,start_execution,end_execution,total_records,state, dynamodb_client)
              
            # Movimiento archivos corruptos al bucket de rechazados
            s3_client.put_object(Bucket = os.environ['error_processed_file'], Body = document_content, Key = 'stock_ps/'+file_name)
              
            # Removiendo el archivo EDI procesado del bucket raw
            s3_client.delete_object(Bucket = name_bucket, Key = path_file)
    
    except:

      '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
      e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
      catch_error(traceback, context, dateutil, datetime, boto3)
      raise


''' Función que permite validar si un campo es decimal '''
def field_value_validation(field_value, line_number, field_number, audit_log, validate_num):
    if field_value != '':
        if validate_num:
            try:
                res = float(field_value)
                audit_log = ''
            except ValueError:
                audit_log = error_value.format(line_number, field_number)
        else:
            audit_log = ''
    else:
        audit_log = audit_log.format(line_number, field_number)
        field_value = ''
    return field_value, audit_log
    

''' Función que permite extraer el nombre y la extensión del nombre de un archivo '''
def get_filename_extension(my_file_name):
    myfile, myext = os.path.splitext(my_file_name)
    return myfile, myext.lstrip('.')
    

''' Función que permite guardar auditoria de los EDI recibidos por parte del cliente '''
def register_load_audit(country,file_name,bgm,snrf,ean_provider,ean_trader,load_date,load_day,load_hour,load_month,load_year,path_file,provider,trader,reported_start_date,reported_end_date,processing_state,log_audit,start_execution,end_execution,total_records,state, dynamodb_client):

    file_name, extension_file = get_filename_extension(file_name)
  
    dynamodb_client.put_item(
        TableName='aud_load_audit',
        Item={
            'pk': {'S': 'country#' + validate_none_value(country) + '#document_type#'+ validate_none_value(document_type)},
            'sk': {'S':  'channel#' + validate_none_value(channel) + '#ean_provider#' + validate_none_value(ean_provider) + '#ean_trader#' + validate_none_value(ean_trader) + '#file_name#' + validate_none_value(file_name)},
            'bgm': {'S': bgm},
            'country': {'S': country},
            'ean_provider': {'S': ean_provider},
            'ean_trader': {'S': ean_trader},
            'file_name': {'S': file_name},
            'load_date': {'S': load_date},
            'load_day': {'S': load_day},
            'load_hour': {'S': load_hour},
            'load_month': {'S': load_month},
            'load_year': {'S': load_year},
            'path_file': {'S': os.environ['backup_bucket'] + '/' + path_file},
            'provider': {'S': provider},
            'reported_end_date': {'S': reported_end_date},
            'reported_start_date': {'S': reported_start_date},
            'snrf': {'S': snrf},
            'state': {'S': state},
            'trader': {'S': trader},
            'document_type': {'S': document_type},
            'processing_state': {'S': processing_state},
            'log_audit': {'S': log_audit},
            'start_execution': {'S': start_execution},
            'end_execution': {'S': end_execution},
            'total_records': {'S': str(total_records)},
            'file_name_log': {'S': ''},
            'path_file_log': {'S': ''},
            'path_file_sftp': {'S': ''},
            'channel': {'S': channel},
            'email_load': {'S': 'soporteprescriptiva@gmail.com'},
            'user_load': {'S': 'soporte'},
            'extension_file': {'S': extension_file},
            'details': {'L': []},
            'sk_lsi1': {'S': file_name}
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
  

''' Esta función permite validar si el valor del parámetro "value" tiene el valor "None" y devuelve '' o "value" en caso contrario. '''
def validate_none_value(value):
    return value if value is not None else ''

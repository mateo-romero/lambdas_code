import json
import urllib.parse
import os
import boto3
import awswrangler as wr
import pandas as pd
import io
from datetime import datetime, timedelta
import dateutil
import traceback
from catch_error import catch_error
import locale
import re



# realizamos conexión con dynamodb 
dynamodb = boto3.client("dynamodb")

# declaramos la variables constantes qye se usuaran el codigo
STATE_IN_PROCESS = 'EN PROCESO'
STATE_REJECTED_BY_STRUCTURE = 'RECHAZADO POR ESTRUCTURA'
STATE_TOTAL_LOAD = 'CARGA TOTAL'
STATE_PARTIAL_LOAD = 'CARGA PARCIAL'
STATE_SYSTEM_ERROR = 'ERROR SISTEMA'   
    
'''
obtenemos los nombre de los bucket
'''
#bucket 
bucket_log = os.environ['bucket_log']
bucket_sales_parquet = os.environ['bucket_sales_parquet']
bucket_stock_parquet = os.environ['bucket_stock_parquet']
bucket_backup = os.environ['bucket_backup']
bucket_rejected = os.environ['bucket_rejected']



'''
Funcion principal para el mapeo de archivos externos
'''
def lambda_handler(event, context):

    #iniciamos el dataFrame para el log
    df_log = pd.DataFrame()  

    timeZone = dateutil.tz.gettz('America/Bogota')
    # fecha actual
    current_date = datetime.now(tz=timeZone)

    # creamos un diccionario con los datos complementarios que se necesitan para generar la data
    dict_aud_load_audit = {
      'current_date'   : current_date,
      'load_date'      : current_date.strftime('%Y%m%d'),
      'load_time'      : current_date.strftime('%H%M%S'),
      'load_year'      : str(current_date.year),
      'load_month'     : str(current_date.month).zfill(2),
      'load_day'       : str(current_date.day).zfill(2),
      'dtm_report_date': current_date.strftime('%Y%m%d'),
      'bgm'            : current_date.strftime('%Y%m%d%H%M%S'),
      'sendreference'  : current_date.strftime('%Y%m%d%H%M%S'),
      'bucket_log'     : '',
      'log_file_name'  : '',
      'time_zone'      : 'America/Bogota',
      'path_file'      : '',
      'total_records'  : 0,
      'sender'         : '',
      'receiver'       : '',
      'country'        : '',
      'details'        : [],
    }    
    
    try:
        # Obtenemos el el nombre del bucket de s3
        name_bucket = event['Records'][0]['s3']['bucket']['name']
        
        # Obtenemos las subcarpetas y el nombre del archivo cargado
        path_file = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        
        # Obtenemos del path del archivo la informacion necesaria para realizar la consulta de la configuracion
        dict_path_file = get_information_path_file(path_file)

        # consultamos las empresas fabricantes y comerciantes
        company_sender = get_company_name('C', dict_path_file.get('ean_sender'))
        company_receiver = get_company_name('F', dict_path_file.get('ean_receiver'))

        # sacamos el pais el nombre del fabricante y comerciante
        dict_aud_load_audit['sender']    = company_sender.get('company_name').get('S')
        dict_aud_load_audit['receiver']  = company_receiver.get('company_name').get('S')
        dict_aud_load_audit['country']   = company_receiver.get('country').get('S')
        dict_aud_load_audit['time_zone'] = get_time_zone_file(dict_aud_load_audit.get('country'))

        # actaulizamos el estado del registro a en proceso
        update_state_load(dict_path_file, dict_aud_load_audit)

        # Obtemeos la configuracion de mapeo de archivos
        dict_configuration = get_configuration_mapping_file(dict_path_file)
        
        # verificamos si se tiene configurada el mapeo para archivos
        if dict_configuration != None:
            
            # Validamos la extructura del archivo
            errors, message, df_sales_stock = validate_extructure_file(name_bucket, path_file, dict_path_file, dict_configuration)

            #Validamos si encontramos errores a nivel de extructura del archivo
            if errors == False:

                # validamos que se tenga contratado el producto ventas e inventarios entre el emisor y receptor
                if get_validate_contrated_product(dict_aud_load_audit, dict_path_file):
                                  
                  #sacamos la cantidad de resgitros que tien el archivo
                  dict_aud_load_audit['total_records'] = df_sales_stock.shape[0]
                  
                  #iniciamos validaciones a nivel de registros
                  df_sales_stock_validation, array_column_name_by_log = get_validation_estructure_records(df_sales_stock, dict_configuration, dict_path_file)

                  # Obtenemos los registros ok y los que tiene error y el estado en quedara el archivo
                  df_sales_stock_ok, df_sales_stock_errors = get_separation_data_ok(df_sales_stock_validation)
                  
                  #obtenemos los detalle de cantidades y valores del archivo
                  dict_aud_load_audit['details'] = calculate_details_sales_stock(df_sales_stock_ok)

                  # generamos los archivos parquet con la infromacion de ventas e inventarios
                  generate_parquet(df_sales_stock_ok, dict_aud_load_audit, dict_path_file)

                  # validamos el estado del proceso del archivo y generamos los log
                  validate_state_load(df_sales_stock_ok, df_sales_stock_errors, dict_aud_load_audit,name_bucket,path_file,dict_path_file, array_column_name_by_log)

                else:
                  
                  # mensaje no contratado
                  message = 'El proveedor no tiene contratado el servicio de \"Archivos Externos\" con el comercio {0}'.format(dict_aud_load_audit.get('sender'))

                  #Obtenemos el dataFrame con el log
                  df_log = build_data_log(df_log, message)

                  # actaulizamos y movemos los archivos
                  process_errors_records(dict_path_file, df_log, dict_aud_load_audit, name_bucket, path_file, bucket_backup, STATE_REJECTED_BY_STRUCTURE)
         

            else:
              
              #Obtenemos el dataFrame con el log
              df_log = build_data_log(df_log, message)

              # actaulizamos y movemos los archivos
              process_errors_records(dict_path_file, df_log, dict_aud_load_audit, name_bucket, path_file, bucket_rejected, STATE_REJECTED_BY_STRUCTURE)
         
        # NO existe configuracion para el mapeo de archivos externos
        else:
            
            # mensaje de error presentado
            message = '''No se ha configurado el mapeo de los campos entre la empresa emisora {0} y la empresa receptora {1}'''.format(
                        company_sender.get('company_name').get('S'), company_receiver.get('company_name').get('S'))

            #Obtenemos el dataFrame con el log
            df_log = build_data_log(df_log, message)

            # actaulizamos y movemos los archivos
            process_errors_records(dict_path_file, df_log, dict_aud_load_audit, name_bucket, path_file, bucket_rejected, STATE_REJECTED_BY_STRUCTURE)
        
    except:

      try:

        # copy and remove file
        copy_and_delete_file(name_bucket, path_file, bucket_rejected, dict_path_file, dict_aud_load_audit)

        # update load aud
        update_load_audit(dict_aud_load_audit, dict_path_file, STATE_SYSTEM_ERROR)

        '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
        e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
        catch_error(traceback, context, dateutil, datetime, boto3)
        raise
        
      except:

        '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
        e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
        catch_error(traceback, context, dateutil, datetime, boto3)
        raise



'''
Funcion que se encarga de validar el estado en el que quedo el procesamiento del archivo actualizar la tabla de auditoria
y generar log y mover los archivos
@Params df_sales_stock_ok dataFrame con los registros que estan ok
@Params dict_path_file dicionario con la informacion nasica del archivo cargado
@Params df_sales_stock_errors dataDrame con los registros que tienen errores
@Params dict_aud_load_audit diccionario con la informacion complementaria
@Params name_bucket bucjet orign del archivo
@Params path_file ruta origen del archivo
@Params array_column_name_by_log listado con el nombre de columnas que el cliente envio en el archivo
'''
def validate_state_load(df_sales_stock_ok, df_sales_stock_errors, dict_aud_load_audit,name_bucket,path_file,dict_path_file, array_column_name_by_log):
    
  # total de registros ok
  records_ok = df_sales_stock_ok.shape[0]
 
  # total de registros malos
  records_error = df_sales_stock_errors.shape[0]

  # seleccionamos solo las columnas que el cliente envio en el archivo
  df_sales_stock_errors = df_sales_stock_errors[array_column_name_by_log]

  # todo registro ok
  if records_ok == dict_aud_load_audit.get('total_records'):

    process_ok_records(dict_path_file, dict_aud_load_audit, name_bucket, path_file, bucket_backup, STATE_TOTAL_LOAD)

  # todo registro con error
  elif records_error == dict_aud_load_audit.get('total_records'):

    process_errors_records(dict_path_file, df_sales_stock_errors, dict_aud_load_audit, name_bucket, path_file, bucket_rejected, STATE_REJECTED_BY_STRUCTURE)
  
  # registros buenos y malos
  else:

    process_errors_records(dict_path_file, df_sales_stock_errors, dict_aud_load_audit, name_bucket, path_file, bucket_backup, STATE_PARTIAL_LOAD)
    


'''
Funcion que se encarga de procesar los registros que tiene error
@Params dict_path_file dicionario con la informacion nasica del archivo cargado
@Params df_sales_stock_errors dataDrame con los registros que tienen errores
@Params dict_aud_load_audit diccionario con la informacion complementaria
@Params name_bucket bucjet orign del archivo
@Params path_file ruta origen del archivo
@Params bucket_destino bucket destino donde se dejara el archivo
@Params state estado en el que quedo el archivo
'''
def process_errors_records(dict_path_file, df_sales_stock_errors, dict_aud_load_audit, name_bucket, path_file, bucket_destino, state):

    # guardo el log
    save_log_file(dict_path_file, df_sales_stock_errors, dict_aud_load_audit) 

    # copy an remove file
    copy_and_delete_file(name_bucket, path_file, bucket_destino, dict_path_file, dict_aud_load_audit)

    # update load aud
    update_load_audit(dict_aud_load_audit, dict_path_file, state)



'''
funcion que se encarga de procesar y actulizar la tabla de load audit con los registros que estan todos ok
@Params dict_path_file dicionario con la informacion nasica del archivo cargado
@Params dict_aud_load_audit diccionario con la informacion complementaria
@Params name_bucket bucjet orign del archivo
@Params path_file ruta origen del archivo
@Params bucket_destino bucket destino donde se dejara el archivo
@Params state estado en el que quedo el archivo
'''
def process_ok_records(dict_path_file, dict_aud_load_audit, name_bucket, path_file, bucket_destino, state):

    # copy an remove file
    copy_and_delete_file(name_bucket, path_file, bucket_destino, dict_path_file, dict_aud_load_audit )

    # update load aud
    update_load_audit(dict_aud_load_audit, dict_path_file, state)



'''
Esta funcion se encarga de obtener del path de archivo los siguientes datos
los cuales son necesario para continuar con el procesamiento del archivo
1. emisor
2. receptor
3. tipo de archivo cargado
4. nombre del archivo
5. extension del archivo
@Params path_file ruta del archivo en s3
'''
def get_information_path_file(path_file):
   
    # Primero hacemos una separacion por / para obtener las carpetas y nombre de archivo
    array_data_path = path_file.split('/')
    
    # Obtenemos el nombre del archivo
    file_name = array_data_path[3]
    
    # cuando el archivo es de ventas e iventarios quitamos el _ de SALES_STOCK a SALESSTOCK para que no afecte obteniendo el emisor
    rename_file = file_name.replace('SALES_STOCK','SALESSTOCK')

    # separamos el nombre del archivo por el _ para obtener el emisor y el receptor 
    array_file_name = rename_file.split('_')

    # Obtenemos la extension del archivo
    extension_file = os.path.splitext(file_name)[1][1:]
    
    #generamos un diccionario Con los datos obtenemos del path de archivo
    dict_path_file = {
        
        'file_name'     : file_name.replace('.' + extension_file,''),
        'type_file'     : array_data_path[2],
        'extension_file': extension_file,
        'ean_sender'    : array_file_name[-4],
        'ean_receiver'  : array_file_name[-3],
        'channel'       : array_data_path[1]
    }
    
    # regresamos diccionario
    return dict_path_file



'''
Funcion que se encarga de realizar la consulta hacia athena de la configuracion que se tiene
para el mapeo de archivos externos segun el emisor con el receptor y tipo de archivo
@Params dict_path_file diccionario con los datos del receptor, emisor, tipo de archivo
'''
def get_configuration_mapping_file(dict_path_file):
    
    # variable que se guardara la configuracion del mapeo de archivos
    dict_configuration = None
    
    #realizamos la consulta hacia dynamoDB
    response = dynamodb.query(
        TableName= 'conf_mapping_external_files',
        KeyConditionExpression= 'pk = :value1',
        FilterExpression= 'ean_receiver = :value2 and ean_sender = :value3',
        ExpressionAttributeValues={
            ':value1': {'S': 'document_type#' + dict_path_file.get('type_file')},
            ':value2': {'S': dict_path_file.get('ean_receiver')},
            ':value3': {'S': dict_path_file.get('ean_sender')}
        }
    )
    
    # verificamos si exite una configuracion
    if response['Items']:
        
        # Obtenemos los datos de la consulta
        dict_configuration = response['Items'][0]

    # retornamos la configuracion
    return dict_configuration



'''
Funcion que se encarga de consultar el archivo del bucket de s3 
@Params bucket Nombre del bucket donde esta el archivo
@Params path_file Ruta y nombre del archivo
@Params dict_path_file diccionario con los datos obtenidos del archivo
@Params dict_configuration diccionario con la configuracion del emisor y receptor
'''
def get_data_file(bucket, path_file, dict_path_file, dict_configuration):
    
    # varible que determina si el archivo tiene encabezados o no
    header_value = None if dict_configuration.get('data_reading').get('S') == '1' else 0
    
    # verificamos el tipo de archivo que deseamos cargar 
    if dict_path_file.get('extension_file') == 'txt' or dict_path_file.get('extension_file') == 'csv':
        
        #Obtenemos el archivo txt o csv del bucket
        df_sales_stock = wr.s3.read_csv('s3://' + bucket + '/' + path_file,
                                        sep= dict_configuration.get('delimiter').get('S'),
                                        keep_default_na=False, header=header_value, dtype=str)
    
    else:
        
        #Obtenemos el archivo excel del bucket
        df_sales_stock = wr.s3.read_excel('s3://' + bucket + '/' + path_file, keep_default_na=False, header=header_value, dtype=str)

    return df_sales_stock
    


'''
Esta Funcion se encarga de validar si la extructura del archivo cargado esta correcta
si el archivo cargado tiene la extension configurada
que no este vacio
@Params bucket bucket origen del archivo
@Params path_file ruta orifen del archivo
@Params dict_path_file dicionario con la infromacion basica del archivo cargado
@Params dict_configuration dicionario con la configuracion del mapeo de archivos
'''
def validate_extructure_file(bucket, path_file, dict_path_file, dict_configuration):
    
    # variable que nos dice si existen errores en el procesamiento de validaciones del archivo y el mensaje
    errors = False
    message = ''
    
    #declaramos el dataFrame que tendra el contenido del archivo
    df_sales_stock = None
    
    #Validamos si el archivo cargado tiene la extension de archivo configurado
    if dict_path_file.get('extension_file').lower() == dict_configuration.get('file_type').get('S'):
        
        # obtenemos los datos del archivo cargado
        df_sales_stock = get_data_file(bucket, path_file, dict_path_file, dict_configuration)
        
        #verificamos que el archivo tenga datos
        if not df_sales_stock.empty:
            
            #obtenemos el total de columnas del archivo
            total_columns = df_sales_stock.shape[1]
            
            # verificamos si el total de columnas es como minimo las que son obligatorias que son 4
            if total_columns < 4:
                
              # agrego el mensaje de error
              message = 'El archivo no tiene el separador de columnas definido en la configuración del mapeo de los campos'
        
              # agrego errores en verdadero
              errors = True
        
        else:
            
            # agrego el mesnaje de error
            message = 'El archivo no puede estar vacío'
        
            # agrego errores en verdadero
            errors = True
        
    else:
        
        # agrego el mesnaje de error
        message = 'El archivo no corresponde al \"Tipo de archivo\" definido en la configuración del mapeo de los campos'
        
        # agrego errores en verdadero
        errors = True
            
    #regresamos la variable errors que nos indica se se presento algun error al momento de realizar la validaciones
    return errors, message, df_sales_stock



'''
funcion que se encarga de guardar el archivo log generado
segun las validaciones realizadas
@Params dict_path_file  datos para generar la ruta del log
@Params df_log dataFrame con el contenido del log
'''    
def save_log_file(dict_path_file, df_log, dict_aud_load_audit):

    # renombramos las columnas del dataframe
    df_log = df_log.rename(
        columns = {
          'errors': 'Error descripcion',
        }
      )
    
    #bucjet base del archivo
    bucket = bucket_log + '/' + dict_path_file.get('type_file')

    # construimos el path node se guardara el log
    log_file_name = dict_path_file.get('file_name') + '_log.csv'
    
    # Transformación del dataframe a archivo csv y cargue del archivo en bucket definido   
    wr.s3.to_csv(df = df_log, path = 's3://' + bucket + '/' + log_file_name, sep = ';', header=True, index=False)

    #regreso el buclet y nombre del archivo log
    dict_aud_load_audit['bucket_log'] = bucket
    dict_aud_load_audit['log_file_name'] = log_file_name

    return dict_aud_load_audit



'''
Funcion que se ebxraga de construir el dataFrame con el mensaje de error 
@Params  message mensaje de error generado
@Params dataFrame donde con la informacion del log
'''
def build_data_log(df_log, message):

  df_log['errors'] = [message]
  df_log['line'] = ['']
  df_log['store_name'] = ['']

  return df_log



'''
Funcion que se encarga de consultar el nombre de la compañias 
@Params type_company typo de compañia a consultar
@Params ean_company Ean de la compañia a consultar
'''
def get_company_name(type_company, ean_company):
        
    #realizamos la consulta hacia dynamoDB
    response = dynamodb.query(
        TableName= 'gen_companies',
        KeyConditionExpression= 'pk = :value1',
        FilterExpression= 'ean_company = :value2',
        ExpressionAttributeValues={
            ':value1': {'S': 'company_type#' + type_company},
            ':value2': {'S': ean_company}
        }
    )
    
    # se regresa el nombre de la compañia
    return response['Items'][0]



'''
Funcion que se encarga de realizar todas la validaciones logicas a nivel de registros del archivo
@Params df_sales_stock data frame con los datos del archivo
@Params dict_configuration diccionario con la configuracion del mapeo
@Params dict_path_file diccionario con los datos basicos de archivo cargado
'''  
def get_validation_estructure_records(df_sales_stock_file, dict_configuration, dict_path_file):
    
    #Obtenemos le configuracion para las columnas
    array_fields = dict_configuration.get('fields')

    # Obtenemos el formato de fecha de ka configuracion de de campos
    format_date = next((field_config['M']['format']['S'] for field_config in array_fields['L'] if field_config['M']['field']['S'] == 'report_date'), None)
    
    # Obtenemos el dataFrame con los campos mapeados y nombres de columnas que el cliente envio
    df_sales_stock_validation, array_column_name_by_log = get_build_data_frame_validation(array_fields, df_sales_stock_file)
 
    # paso la columna fecha a string para trabajar las fechas
    df_sales_stock_validation['Fecha reporte'] = df_sales_stock_validation['Fecha reporte'].astype(str)

    # agregamos el numero de linea
    df_sales_stock_validation['line'] = range(1, len(df_sales_stock_validation) + 1)

    # agregamos el emisor y receptor y pais al dataFrame
    df_sales_stock_validation['EAN Emisor'] = dict_path_file.get('ean_sender')
    df_sales_stock_validation['EAN Receptor'] = dict_path_file.get('ean_receiver')
    df_sales_stock_validation['country'] = dict_configuration.get('country').get('S')
    df_sales_stock_validation['errors'] = ''

    #validar fecha del reporte valida
    df_sales_stock_validation = validate_date(df_sales_stock_validation, format_date)

    #declaramos un arreglo con los nombres de las columnas a validar como obligatorias
    array_column_name_requerided = ['EAN Punto de venta', 'EAN Producto', 'Fecha reporte']
 
    #Validaciones si se mapeo detalle a tienda esto aplica solo para ventas 
    if('store_detail' in dict_configuration and dict_configuration.get('store_detail').get('S') == 'SI'):

      # si es detalle a tiendo agregamos los demas campos
      array_column_name_requerided += ['Código identificación tienda', 'Nombre tienda', 'Código vendedor', 'Nombre vendedor', 'Código ciudad']
        
    # validamos las columnas
    validate_empty_field(df_sales_stock_validation, array_column_name_requerided)

    # Validacion caracteres especiales
    validate_special_characters(df_sales_stock_validation)

    #validamos las cantidades segun el archivo cargado
    validate_quantities_field(df_sales_stock_validation, dict_path_file.get('type_file'))
    
    # Identificar duplicados a nivel de registro
    duplicated = df_sales_stock_validation.duplicated(subset=['EAN Emisor', 'EAN Receptor', 'EAN Punto de venta', 'EAN Producto', 'Fecha reporte'], keep=False)

    # Modificar la columna "error" para los registros duplicados
    df_sales_stock_validation.loc[duplicated, 'errors'] += '|No se puede tener más de una vez el mismo (EAN Emisor + EAN Receptor + Punto de Venta + EAN Producto/SKU + Fecha del reporte'
    
    return df_sales_stock_validation, array_column_name_by_log



'''
Funcion que se encarga de validar las cantiddes de ventas e inventarios enviadas sea validas
@Params df_sales_stock_validation dataframe con los datos del archivo
@Params type_file typo de archivo cargado
'''
def validate_quantities_field(df_sales_stock_validation, type_file):

  # declaramos las columnas para validar  ventas
  array_column_name_number_sales = ['Cantidad ventas','Precio Unitario Venta','Valor Total Venta','Valor total neto']
  
  # declaramos las columnas para validar inventarios
  array_column_name_number_stock = ['Cantidad inventarios','Precio Unitario Inventario','Cantidad en tránsito','Valor Total Inventario']
  
  #TODO validar las cantidades si el archivo es combinado
  if type_file == 'sales_stock':

    #validamos que uno de los campos de ventas o inventarios tenga datos
    df_sales_stock_validation.loc[((df_sales_stock_validation['Cantidad ventas'].isnull() | (df_sales_stock_validation['Cantidad ventas'].astype(str).str.strip() == ''))
                                  & (df_sales_stock_validation['Cantidad inventarios'].isnull() | (df_sales_stock_validation['Cantidad inventarios'].astype(str).str.strip() == ''))),
                                  'errors'] += '|El campo [Cantidad ventas] [Cantidad inventarios] no puede estar vacío.'
    
    # valido valores para ventas validos
    validate_opcional_number(df_sales_stock_validation, array_column_name_number_sales, type_file, 'sales')

    # valido valores para inventarios validos
    validate_opcional_number(df_sales_stock_validation, array_column_name_number_stock, type_file, 'stock')

  elif type_file == 'sales':

    # validar que no estan vacios
    validate_empty_field(df_sales_stock_validation, ['Cantidad ventas'])

    # valido que la cantidad sea valida
    validate_opcional_number(df_sales_stock_validation, array_column_name_number_sales, type_file, 'sales')

  else:

    # validar que no estan vacios
    validate_empty_field(df_sales_stock_validation, ['Cantidad inventarios'])

    # validamos que la cantidad se valida
    validate_opcional_number(df_sales_stock_validation, array_column_name_number_stock, type_file, 'stock')



'''
Funcion que se encarga de validar si ek valir numero enviado eb valido
@Params df_sales_stock_validation datagrame con los datos del archivo
@Params column_name nombre de la columna a evaluar
@Params type_file tipo de archivo a validar
@Params type_column tipo de columnas a validar para ventas o para inventarios
'''
def validate_numeric_field(df_sales_stock_validation, column_name, type_file, type_column, errors_column='errors'):
  
  # Convertimos los valores de la columna a numéricos
  df_sales_stock_validation['value_num'] = pd.to_numeric(df_sales_stock_validation[column_name], errors='coerce')

  # Condición para verificar si el archivo es de ventas e inventarios
  if type_file == 'sales_stock':
  
    # Verificamos si la columna es 'Cantidad ventas' o 'Cantidad inventarios'
    if column_name in ['Cantidad ventas', 'Cantidad inventarios']:

      # condicion de validacion para cantidades
      condition = df_sales_stock_validation[column_name].notna() & \
                  df_sales_stock_validation[column_name].astype(str).str.strip().ne('') & \
                  df_sales_stock_validation['value_num'].isna()
    else:
    
      # Validación para columnas que no sean 'Cantidad ventas' o 'Cantidad inventarios' y que la columna tenga datos
      if df_sales_stock_validation[column_name].notna().any():
        
        # verificaamos si vamos a validar columna de ventas
        if type_column == 'sales':
          
          # agregamos valor de ventas a validar
          quantity_column = 'Cantidad ventas'
                
        else:
          
          # agregamos valor de inventarios a validar
          quantity_column = 'Cantidad inventarios'

        # condicion de la validsacion      
        condition = df_sales_stock_validation[quantity_column].notna() & \
                    df_sales_stock_validation[quantity_column].astype(str).str.strip().ne('') & \
                    df_sales_stock_validation['value_num'].isna()
      else:
    
        # Si la columna no tiene datos, no hacemos nada    
        return  
  else:
    
    # Validación general para archivos que no sean combinados de ventas e inventarios
    if df_sales_stock_validation[column_name].notna().any():
      
      # codicion a evaluar
      condition = df_sales_stock_validation['value_num'].isna()

    else:
  
      return  # Si la columna no tiene datos, no hacemos nada

  # Aplicamos la condición para agregar el mensaje de error
  df_sales_stock_validation.loc[condition, errors_column] += f'|El campo {column_name} no cumple con el formato definido en el mapeo de los campos'

     
    
'''
Funcion que se encarga de validar si el valor numero enviado tiene separador de decimales correcto
@Params df_sales_stock_validation datagrame con los datos del archivo
@Params column_name nombre de la columna a evaluar
'''
def validate_decimal_field(df_sales_stock_validation, column_name, errors_column='errors'):

  # se consulta registros que contengan una coma en el valor y agrego el mensaje de error
  df_sales_stock_validation.loc[((df_sales_stock_validation[column_name].notna()) &
                                 (df_sales_stock_validation[column_name].astype(str).str.contains(','))),
                                  errors_column] += f'|La {column_name} para reportar no debe tener separador de miles, el separador de decimales debe ser punto (.)'



'''
Funcion que se encarga de validar si el dato enviado esta vacio
@Params df_sales_stock_validation datagrame con los datos del archivo
@Params column_name nombre de la columna a evaluar
'''
def validate_empty_field(df_sales_stock_validation, array_column_name, errors_column='errors'):

  #recorremos el listado de campos para validar la columna
  for column_name in array_column_name:

    # verfico si existen datos vacios y agrego el mensaje de error el registro
    df_sales_stock_validation.loc[((df_sales_stock_validation[column_name].isnull()) |
                                   (df_sales_stock_validation[column_name].astype(str).str.strip() == '') |
                                   (df_sales_stock_validation[column_name].isna())), errors_column] += f'|El campo [{column_name}] no puede estar vacío.'



'''
Funcion que se encarga de validar los valores opcionales con formato numerico enviado
@Params df_sales_stock_validation datagrame con los datos del archivo
@Params column_name nombre de la columna a evaluar
@Params type_file tipo de archivo a procesar el valor por defecto es sales este parametro se utiliza para cuendo el archivo es combinado ventas e inventarios
'''
def validate_opcional_number(df_sales_stock_validation, array_column_name, type_file, type_column):

  #recorremos el listado de nombres de columnas
  for column_name in array_column_name:

    # valido que el separador de decimales este correcto
    validate_decimal_field(df_sales_stock_validation, column_name)

    # valido que sea un numero valido
    validate_numeric_field(df_sales_stock_validation, column_name, type_file, type_column)



'''
Funcion que se encarga de crear un nuevo dataFrame con las colunas configuradas
en la tabla de mapeo y Obtener los registros del archivo cargado para cada columna
@Params array_fields arragleglo con los campos mapeados
@Params df_sales_stock con lo datos del archivo cargado
'''
def get_build_data_frame_validation(array_fields, df_sales_stock_file):
    
    # declaro el dataFrame de validacion
    df_sales_stock_validation = pd.DataFrame()
    df_sales_stock_validation['errors'] = ''
    df_sales_stock_validation['line'] = ''
    df_sales_stock_validation['EAN Punto de venta'] = None
    df_sales_stock_validation['Fecha reporte'] = None
    df_sales_stock_validation['Cantidad ventas'] = None
    df_sales_stock_validation['Cantidad inventarios'] = None
    df_sales_stock_validation['Precio Unitario Venta'] = None
    df_sales_stock_validation['Precio Unitario Inventario'] = None
    df_sales_stock_validation['Código interno'] = None
    df_sales_stock_validation['Cantidad en tránsito'] = None
    df_sales_stock_validation['Valor Total Venta'] = None
    df_sales_stock_validation['Valor Total Inventario'] = None
    df_sales_stock_validation['Código identificación tienda'] = None 
    df_sales_stock_validation['Nombre tienda'] = None 
    df_sales_stock_validation['Código vendedor'] = None 
    df_sales_stock_validation['Nombre vendedor'] = None 
    df_sales_stock_validation['Código ciudad'] = None 
    df_sales_stock_validation['Nombre formato tienda'] = None 
    df_sales_stock_validation['Valor total neto'] = None 
    df_sales_stock_validation['Código postal'] = None 

    # creamos un diccionario de datos con las columnas que pueden llegar en el arreglo
    field_name_mapping = { 'ean_point_sale'  : 'EAN Punto de venta',
                           'ean_product'     : 'EAN Producto',
                           'report_date'     : 'Fecha reporte',
                           'sales_quantity'  : 'Cantidad ventas',
                           'stock_quantity'  : 'Cantidad inventarios',
                           'sales_price_net' : 'Precio Unitario Venta',
                           'stock_price_net' : 'Precio Unitario Inventario',
                           'internal_code'   : 'Código interno',
                           'qty_on_transit'  : 'Cantidad en tránsito',
                           'sales_price_list': 'Valor Total Venta',
                           'stock_price_list': 'Valor Total Inventario',
                           'store_id_code'   : 'Código identificación tienda',
                           'store_name'      : 'Nombre tienda',
                           'seller_code'     : 'Código vendedor',
                           'seller_name'     : 'Nombre vendedor',
                           'city_code'       : 'Código ciudad',
                           'store_format_name': 'Nombre formato tienda',
                           'total_net_value' : 'Valor total neto',
                           'postal_code'     : 'Código postal',

                         }
    
    #total de columnas encontradas en el archivo
    total_columns = df_sales_stock_file.shape[1]

    # declamos un arreglo el cual contendra las columnas para generar el log si se presenta
    array_column_name_by_log = ['errors', 'line']

    #recorremos el listado de campos configurados
    for field_config in array_fields['L']:

      # Obtemos el nombre del campo mapeado
      field = field_config['M']['field']['S']

      # Ontenemos la posicion en la que debe llegar el campo
      position_field = int(field_config['M']['position']['S'])    

      # Obtenemos el nombre de la columna para el dataFrame
      field_name = field_name_mapping.get(field)

      #validamos que la poicion mapeada se encuentre en el dataFrame
      if position_field <= total_columns:

        # agregamos las columnas que el cliente envio
        array_column_name_by_log.append(field_name)

        # agregamos al dataFrame los valores obtenidos del archivo
        df_sales_stock_validation[field_name] = df_sales_stock_file.iloc[:, position_field - 1]

    # regresamos el formato y la posicion
    return df_sales_stock_validation, array_column_name_by_log



'''
Funcion que se encarga de realizar todas la validaciones que tiene que ver con las fechas 
segun la configuracion mapeada vs las fechas reportada
@Params df_sales_stock_validation dataframe con los datos del archivo
@Params format_date formato fecha configurado por el cliente
'''
def validate_date(df_sales_stock_validation, format_date):
 
  #agrupamos las fecha reportadas
  array_date_unique = df_sales_stock_validation.loc[df_sales_stock_validation['Fecha reporte'].notna() & 
                                                    df_sales_stock_validation['Fecha reporte'] != '' , 'Fecha reporte'].unique()

  df_dates_report = pd.DataFrame(columns=('Fecha reporte','error_date', 'tmp_date'))

  # paso la columna fecha a strin ara trabajar la fechas
  df_dates_report['Fecha reporte'] = df_dates_report['Fecha reporte'].astype(str)
  
  # recorremos la fecha
  for index, date in enumerate(array_date_unique):

    # pasamos la fecha a string
    date_str = str(date)
    
    # Obtenemos el valor de la fecha 
    date_report = df_sales_stock_validation.loc[index]

    # validamos que la fecha sea valida
    error_date = validate_date_report(date_str, format_date)

    # Obtenemos ka fecha en que se esta reportando la data
    date_tmp = get_date_reported(date_str)

    # se agragega a un dataframes las validaciones de las fechas
    df_date_tmp = pd.DataFrame({ 'Fecha reporte': [date], 'error_date': [error_date], 'tmp_date': [date_tmp] })
 
    df_dates_report = pd.concat([df_dates_report,df_date_tmp], ignore_index=True)

  # agregamos al dataframa original la validaciones de la fecha  
  df_sales_stock_validation = df_sales_stock_validation.merge(df_dates_report, on='Fecha reporte', how = 'inner')

  #scamos un dataFrame con los errores presentados
  df_tmp_errors =  df_dates_report[(df_dates_report.error_date.notna()) & (df_dates_report.error_date!= '')].copy()

  #verificamos que se tengas errores para concatenalos a la columnas de errores
  if not df_tmp_errors.empty:

    # se concatena el error presentado en la fecha
    df_sales_stock_validation['errors'] += df_sales_stock_validation['error_date']  

  return df_sales_stock_validation



'''
Funcion para valiodar las fechas del archivo
@Params fecha_reporte  fecha a evaluar
@Params format_date Formato de la fecha
'''
def validate_date_report(fecha_reporte, format_date):

    date = None
    message = ''

    # paso ka fecha a string para evaluar
    fecha_reporte = str(fecha_reporte)

    # TODO: Validaciones según el formato configurado
    if format_date == 'AAAA + Semana ISO':

      date = validate_iso_week_format(fecha_reporte)

    elif format_date == 'AAAAMMDD':

      date = validate_date_format(fecha_reporte, '%Y%m%d')

    else:

      date = validate_date_format(fecha_reporte, '%d%m%Y')

    if date is None:

      message = f'|El campo Fecha reporte no cumple con el formato definido en el mapeo de los campos'
    
    return message



'''
Funcion que se encarga de validar fecha en formato ISO
@Params fecha_reporte fecha a validar el formato
@Params errors errores que se presenten 
'''
def validate_iso_week_format(fecha_reporte):
    
    date = None

    # verficamos que la fecha tenga 6 caracteres
    if len(fecha_reporte) == 6:

        try:
            # se saca el año y la sema de la fecha
            ano = int(fecha_reporte[:4])
            semana = int(fecha_reporte[4:6])

            # obtenemos la fecha con el primer dia de esa semana si la semana es valida de lo contrario no se obtiene
            date = datetime.fromisocalendar(ano, semana, 1)

        except ValueError:
          
          date = None
    return date



'''
Funcion para validar fecha en formato añño mes dia p dia mes año
@Params fecha_reporte fecha a validar el formato
@Params format_str formato de fecha a validar
@Params errors errores que se presenten 
'''
def validate_date_format(fecha_reporte, format_str):

    date = None
    
    # verifcamos que la fecha tenga 8 caracteres
    if len(fecha_reporte) == 8:

        try:

          # Sacamos la fecha en el formato enviado si la fecha no esta en ese formato no es valida la fecha y no se regersa nada        
          date = datetime.strptime(fecha_reporte, format_str)        
        
        except ValueError:
                    
          date = None

    return date



'''
Funcion que se encarga de separar los regjistros que estan con error de los que estan ok 
y verificar el que estado quedara el cargue del archivo
@Params df_sales_stock_validation dataFrame con los datos validados
'''
def get_separation_data_ok(df_sales_stock_validation):

  # Obtenemos los registros con error
  df_sales_stock_errors = df_sales_stock_validation[df_sales_stock_validation.errors != ''].copy()

  # Obtenemos los registros que estan correctos
  df_sales_stock_ok = df_sales_stock_validation[df_sales_stock_validation.errors == ''].copy()

  return df_sales_stock_ok, df_sales_stock_errors



'''
Funcion que se encarga de pasar cualquier fecha a formato AAAAMMDD
@Params date_reported datos del registro fecha
'''
def get_date_reported(date_reported):  

  # obtengo y paso la fecha a string
  date_reported = str(date_reported)

  # verifico si la fecha viene en formato iso
  if len(date_reported) == 6:

    try:
      
      ano = int(date_reported[:4])
      semana = int(date_reported[4:6])
    
      # se Obtiene el primer dia de la semana segun la semana reportada
      date = datetime.fromisocalendar(ano, semana, 1)

      # calculo el último día de la semana sumando 6 días al primer día de la semana
      date = (date + timedelta(days=6)).strftime('%Y%m%d')

    except ValueError:

      date = ''

  else:

    try:
        
        # se intenta pasar la fecha a formato ano mes dia
        date = datetime.strptime(date_reported, '%Y%m%d')

        # Devuelve la fecha en el formato deseado (AAAAMMDD)
        date = date_reported     

    except ValueError:

      try:

        # se intenta pasar la fecha a formato dia mes año
        date_tmp = datetime.strptime(date_reported, '%d%m%Y')
      
        # Devuelve la fecha en el formato deseado (AAAAMMDD)
        date = date_tmp.strftime('%Y%m%d')      

      except ValueError:

        date = ''

  return date



'''
Funcion que se encarga de generar los archivos parquet para ventas o inventarios 
con la data que esta ok de pues de validada
@Params df_sales_stock_ok dataframe con los registros que estan buenos para ser cargados
@Params dict_aud_load_audit diccionario con la complementaria de actualuzaciones
@Params dict_path_file doccionario con la informacion basica del archivo cargado
'''
def generate_parquet(df_sales_stock_ok, dict_aud_load_audit, dict_path_file):
  
  # Agregamos fechas de carga para el registro
  df_sales_stock_ok['load_date']       = dict_aud_load_audit.get('load_date')
  df_sales_stock_ok['load_time']       = dict_aud_load_audit.get('load_time')
  df_sales_stock_ok['dtm_report_date'] = dict_aud_load_audit.get('dtm_report_date')

  # Agregamos bgm y sendreference para el registro
  df_sales_stock_ok['bgm_document']  = dict_aud_load_audit.get('bgm')
  df_sales_stock_ok['sendreference'] = dict_aud_load_audit.get('sendreference')
  df_sales_stock_ok['channel']       = dict_path_file.get('channel')

  # generamos los archivos parquet para ventas e inventarios
  generate_parquet_sales(df_sales_stock_ok, dict_path_file, dict_aud_load_audit)

  # generamos los archivos parquet para inventarios
  generate_parquet_stock(df_sales_stock_ok, dict_path_file, dict_aud_load_audit)
  


'''
Funcion que se encarga de generar el archivo parquet para ventas y guardarlo en bucket de s3
para ser procesado y cargado
@Params dataframe con los registros que estan buenos para ser cargados
@Params dict_path_file doccionario con la informacion basica del archivo cargado
@Params dict_aud_load_audit doccionario con la informacion de datos complementarios
'''
def generate_parquet_sales(df_sales_stock_ok, dict_path_file, dict_aud_load_audit):
    
    #ruta donde se dejara el archivo parquetizado
    path_file_parquet = '{0}{1}_{2}_L.parquet'.format(bucket_sales_parquet,
                                                      dict_path_file.get('file_name'),
                                                      dict_aud_load_audit.get('country')
                                                     )

    # pasamos a numero los valores de cantidades para garantizar que sean validos
    df_sales_stock_ok['Cantidad ventas'] = pd.to_numeric(df_sales_stock_ok['Cantidad ventas'], errors='coerce')

    # se obtienen solo los registros que tengan ventas
    df_sales = df_sales_stock_ok[(df_sales_stock_ok['Cantidad ventas'].notna()) & (df_sales_stock_ok['Cantidad ventas'].astype(str).str.strip() != '')].copy()

    # realizamos el calculo de valor unitario y total segun el caso
    df_sales = calculate_values_empty(df_sales, 'Precio Unitario Venta', 'Valor Total Venta', 'Cantidad ventas')

    # verifico que se tengan registros para guardar
    if not df_sales.empty:
      
      # garantizamos que las columnas que se tiene que redondear su valor sean float
      df_sales['Cantidad ventas'] = df_sales['Cantidad ventas'].astype(float)

      # redondeamos las cantidades de ventas a solo 2 decimales
      df_sales['Cantidad ventas'] = df_sales['Cantidad ventas'].round(2)

      # Asignamos el valor del path en que se proceso el archivo
      df_sales['path_of_processed_file'] = path_file_parquet 
      df_sales['document_type'] = dict_path_file.get('type_file')

      # renombrar columnas para ventas
      df_sales = df_sales.rename(
        columns = {
          'EAN Emisor': 'nad_ean_sender',
          'EAN Receptor': 'nad_ean_receiver',
          'EAN Punto de venta': 'det_ean_loc',
          'EAN Producto': 'det_ean_lin',  
          'Cantidad ventas': 'det_qty_sale',
          'Precio Unitario Venta': 'det_pri_neto',
          'Valor Total Venta': 'det_pri_neto_total',
          'tmp_date': 'dtm_sale_date',
        }
      )

      #eliminamos las columnas de inventarios y los de detalle a tienda : NOTA.. detalle a tienda a partir de la columna Código identificación
      # tienda de debe quitar cuando se habilite la funcionalidad
      df_sales.drop(columns=['Precio Unitario Inventario', 'Cantidad inventarios', 'Cantidad en tránsito', 'Valor Total Inventario', 'errors', 'line', 'Fecha reporte', 'error_date','Código identificación tienda', 'Nombre tienda', 'Código vendedor', 'Nombre vendedor', 'Código ciudad', 'Nombre formato tienda','Valor total neto'], axis=1, inplace = True)

      # Construimos los valores para el parquet
      sales_dtypes = {
          'load_date': 'string',
          'load_time': 'string',
          'bgm_document': 'string',
          'country': 'string',
          'nad_ean_receiver': 'string',
          'nad_ean_sender': 'string',
          'dtm_report_date': 'string',
          'path_of_processed_file': 'string',
          'det_ean_lin': 'string',
          'det_qty_sale': 'double',
          'det_ean_loc': 'string',
          'det_pri_neto': 'double',
          'det_pri_neto_total': 'double',
          'dtm_sale_date': 'string',
          'sendreference': 'string',
          'channel':'string',
          'document_type':'string'
      }

      # se genera y se guarda el archivo el bucket para ser procesado
      wr.s3.to_parquet(dtype=sales_dtypes,
                       df=pd.DataFrame(df_sales, columns=['load_date', 'load_time', 'bgm_document', 'country', 'nad_ean_receiver', 'nad_ean_sender', 'dtm_report_date', 'path_of_processed_file', 'det_ean_lin', 'det_qty_sale', 'det_ean_loc', 'det_pri_neto', 'det_pri_neto_total', 'dtm_sale_date', 'sendreference', 'channel', 'document_type']),
                       path=path_file_parquet)



'''
Funcion que se encarga de generar el archivo parquet para inventarios
y dejarlo el bucket para que sea procesado con el glue
@Params dataframe con los registros que estan buenos para ser cargados
@Params dict_path_file doccionario con la informacion basica del archivo cargado
'''
def generate_parquet_stock(df_sales_stock_ok, dict_path_file, dict_aud_load_audit):

    #ruta donde se dejara el archivo parquetizado
    path_file_parquet = '{0}{1}_{2}_L.parquet'.format(bucket_stock_parquet,
                                                dict_path_file.get('file_name'),
                                                dict_aud_load_audit.get('country')
                                               )
    
    # pasamos a numero los valores de cantidades para garantizar que sean validos                                               
    df_sales_stock_ok['Cantidad inventarios'] = pd.to_numeric(df_sales_stock_ok['Cantidad inventarios'], errors='coerce')
    
    # se obtienen solo los registros que tengan inventarios
    df_stock = df_sales_stock_ok[(df_sales_stock_ok['Cantidad inventarios'].notna()) & (df_sales_stock_ok['Cantidad inventarios'].astype(str).str.strip() != '')].copy()

    # realizamos el calculo de valor unitario y total segun el caso
    df_stock = calculate_values_empty(df_stock, 'Precio Unitario Inventario', 'Valor Total Inventario', 'Cantidad inventarios')

    # verifico que se tengan registros para guardar
    if not df_stock.empty:    

      # garantizamos que las columnas que se tiene que redondear su valor sean float
      df_stock['Cantidad inventarios'] = df_stock['Cantidad inventarios'].astype(float)
      
      # redondeamos las cantidades de inventarios a solo 2 decimales
      df_stock['Cantidad inventarios'] = df_stock['Cantidad inventarios'].round(2)

      # Asignamos el valor del path en que se proceso el archivo
      df_stock['path_of_processed_file'] = path_file_parquet
      df_stock['det_pri_list'] = None
      df_stock['det_pri_neto_total'] = None
      df_stock['dtm_stock_final_date'] = df_stock['tmp_date']   
      df_stock['det_ean_lin_upc'] = None
      df_stock['det_pia_in'] = None
      df_stock['det_pia_sa'] = None
      df_stock['det_imd'] = None
      df_stock['det_ali'] = None
      df_stock['document_type'] = dict_path_file.get('type_file')

      # renombrar columnas para inventarios
      df_stock = df_stock.rename(
        columns = {
          'EAN Emisor': 'nad_ean_sender',
          'EAN Receptor': 'nad_ean_receiver',
          'EAN Punto de venta': 'det_ean_loc',
          'EAN Producto': 'det_ean_lin',  
          'Cantidad inventarios': 'det_qty_stock',
          'Precio Unitario Inventario': 'det_pri_neto',
          'tmp_date': 'dtm_stock_initial_date', 
          'Cantidad en tránsito': 'det_qty_in_transit',
        }
      )

      #eliminamos las columnas de ventas y el total de inventarios ya que este se calcula en el glue
      df_stock.drop(columns=['Valor Total Inventario', 'Cantidad ventas', 'Precio Unitario Venta', 'Valor Total Venta', 'errors', 'line', 'Fecha reporte', 'error_date', 'Código identificación tienda', 'Nombre tienda', 'Código vendedor', 'Nombre vendedor', 'Código ciudad', 'Nombre formato tienda','Valor total neto'], axis=1, inplace = True)

      # Construimos valores para el parquet
      stock_dtypes = {
          'bgm_document': 'string',
          'nad_ean_sender': 'string',
          'nad_ean_receiver': 'string',
          'dtm_report_date': 'string',
          'dtm_stock_initial_date': 'string',
          'dtm_stock_final_date': 'string',
          'country': 'string',
          'load_date': 'string',
          'load_time': 'string',
          'det_ean_lin': 'string',
          'det_ean_lin_upc': 'string',
          'det_pia_in': 'string',
          'det_pia_sa': 'string',
          'det_imd': 'string',
          'det_ali': 'string',
          'det_qty_stock': 'double',
          'det_qty_in_transit': 'double',
          'det_ean_loc': 'string',
          'det_pri_neto': 'double',
          'det_pri_list': 'double',
          'path_of_processed_file': 'string',
          'sendreference': 'string',
          'channel':'string',
          'document_type':'string'
      }

      # se genera y se guarda el archivo el bucket para ser procesado
      wr.s3.to_parquet(dtype=stock_dtypes,                       
                       df=pd.DataFrame(df_stock, columns=['bgm_document', 'nad_ean_sender', 'nad_ean_receiver', 'dtm_report_date', 'dtm_stock_initial_date', 'dtm_stock_final_date', 'country', 'load_date', 'load_time', 'det_ean_lin', 'det_ean_lin_upc', 'det_pia_in', 'det_pia_sa', 'det_imd', 'det_ali', 'det_qty_stock', 'det_qty_in_transit', 'det_ean_loc', 'det_pri_neto', 'det_pri_list','path_of_processed_file','sendreference', 'channel', 'document_type']),
                       path=path_file_parquet)



'''
funcion que se encarga de realizar la actualizacion en la tabla de auditoria del estado del archivo
@Params dict_aud_load_audit diccionarioa con la informacion complementaria para la actualizacion
@Params dict_path_file diccionario con la informacion basica dek archivo
@Params state_load estado final del archivo
'''
def update_load_audit(dict_aud_load_audit, dict_path_file, state_load):
  
    # se obtiene la zona horaria
    time_zone_file = dateutil.tz.gettz(dict_aud_load_audit.get('time_zone'))

    # fecha actual
    end_date = datetime.now(tz=time_zone_file).strftime('%Y-%m-%d %H:%M:%S')

    #Se declara el sql de actualizacion
    statement = ''' UPDATE aud_load_audit
                    SET  bgm = ?,
                         country = ?,
                         end_execution = ?,
                         load_date = ?,
                         load_day = ?,
                         load_hour = ?,
                         load_month = ?,
                         load_year = ?,                    
                         path_file = ?,
                         processing_state = ?,
                         provider = ?,
                         reported_start_date = ?,
                         reported_end_date = ?,
                         total_records = ?,
                         trader = ?,
                         file_name_log = ?,
                         path_file_log = ?,
                         snrf = ?,
                         details = ?
                    where pk = ? and  sk = ? '''

    # parametros con los valores a actualizar
    parameters =[ {'S': dict_aud_load_audit.get('bgm')},
                  {'S': dict_aud_load_audit.get('country')},
                  {'S': end_date},
                  {'S': dict_aud_load_audit.get('load_date'),},
                  {'S': dict_aud_load_audit.get('load_day')},
                  {'S': dict_aud_load_audit.get('load_time')},
                  {'S': dict_aud_load_audit.get('load_month')},
                  {'S': dict_aud_load_audit.get('load_year')},
                  {'S': dict_aud_load_audit.get('path_file')},
                  {'S': state_load},
                  {'S': dict_aud_load_audit.get('receiver')},
                  {'S': dict_aud_load_audit.get('dtm_report_date')},
                  {'S': dict_aud_load_audit.get('dtm_report_date')},
                  {'S': str(dict_aud_load_audit.get('total_records'))},
                  {'S': dict_aud_load_audit.get('sender')},
                  {'S': dict_aud_load_audit.get('log_file_name')},
                  {'S': dict_aud_load_audit.get('bucket_log')},
                  {'S': dict_aud_load_audit.get('sendreference')},
                  {'L': dict_aud_load_audit.get('details')},
                  {'S': f'''country#{dict_aud_load_audit.get('country')}#document_type#{dict_path_file.get('type_file')}'''},
                  {'S': f'''channel#{dict_path_file.get('channel')}#ean_provider#{dict_path_file.get('ean_receiver')}#ean_trader#{dict_path_file.get('ean_sender')}#file_name#{dict_path_file.get('file_name')}'''}
                ]
    # Ejecución del query hacía dynamodb
    dynamodb.execute_statement(Statement = statement, Parameters = parameters)



'''
funcion que se encarga de actualizar solo el estado de un registro de la tabla de 
auditoria que nos indica que que el archivo se inicio a procesar
@Params dict_path_file diccionario con la informacion nasica del archivo cargado
@Params dict_aud_load_audit diccionario con la informacion del emisor y receptor
'''
def update_state_load(dict_path_file, dict_aud_load_audit): 

  #Se declara el sql de actualizacion
  statement = 'UPDATE aud_load_audit SET processing_state = ?  where pk = ? and sk = ?'

  # parametros con los valores a actualizar
  parameters =[ {'S': STATE_IN_PROCESS},
                {'S': f'''country#{dict_aud_load_audit.get('country')}#document_type#{dict_path_file.get('type_file')}'''},
                {'S': f'''channel#{dict_path_file.get('channel')}#ean_provider#{dict_path_file.get('ean_receiver')}#ean_trader#{dict_path_file.get('ean_sender')}#file_name#{dict_path_file.get('file_name')}'''}
              ]  

  # Ejecución del query hacía dynamodb
  dynamodb.execute_statement(Statement = statement, Parameters = parameters)



'''
Funcion que se encarga de Obtener la zona horaria el pais de la empresa emisora del archivo
@Params country codigo del pais a consultar la zona horaria
'''
def get_time_zone_file(country):

    #realizamos la consulta hacia dynamoDB
    response = dynamodb.query(
        TableName= 'gen_time_zone',
        KeyConditionExpression= 'pk = :value1',     
        ExpressionAttributeValues={
            ':value1': {'S': 'cod_country#' + country}            
        }
    )

    # se regresa el nombre de la compañia
    return response['Items'][0]['time_zone']['S']



'''
Funcion que se encarga de copiar el archivo que se esta procesando al bucket de copias o al de rechazado
@Params bucket_origin Bucjet de origen del archivo
@Params path_origin Path origen del archivo cargado
@Params bucket_destination bucket destino donde se dejara el archivo
@Params dict_path_file diccionario con la infromacion basica del archivo cargado
@Params dict_aud_load_audit dicionario con los datos complemetarios para actualizaciones del archivo
'''
def copy_and_delete_file(bucket_origin, path_origin,  bucket_destination, dict_path_file, dict_aud_load_audit):
  
  #conexion a s3
  s3 = boto3.client('s3')
  
  #generamos la ruta de carpetas para el archivo de copia
  path_file = '/'.join([   
      'external_files',   
      dict_path_file.get('type_file'),
      f'''country={dict_aud_load_audit.get('country')}''',
      f'''ean_provider={dict_path_file.get('ean_receiver')}''',
      f'''ean_trader={dict_path_file.get('ean_sender')}''',
      f'''year_load={dict_aud_load_audit.get('load_year')}''',
      f'''month_load={dict_aud_load_audit.get('load_month')}''',
      f'''day_load={dict_aud_load_audit.get('load_day')}'''
  ])
    
  #movemos el archivo al Bucket copia o al de rechazados segun las validaciones
  s3.copy_object(CopySource={'Bucket': bucket_origin, 'Key': path_origin},
                   Bucket=bucket_destination, Key=path_file + '/' + dict_path_file.get('file_name') + '.' + dict_path_file.get('extension_file'))

  # eliminamos el archivo que ya fue procesado   
  s3.delete_object(Bucket=bucket_origin, Key=path_origin)

  dict_aud_load_audit['path_file'] = f'{bucket_destination}/{path_file}'

  return dict_aud_load_audit



'''
Funcion que se encarga de validar si un fabricante tiene contratado el producto de ventas o iventarios 
con el comerciante que esta enviando la infromacion
@Params dict_aud_load_audit diccionario con la informacion del pais del fabricante
@Params dict_path_file diccionario con la infromacion del archivo cargado
'''
def get_validate_contrated_product(dict_aud_load_audit, dict_path_file):

  # producto contratado
  contrated_product = False

  #realizamos la consulta hacia dynamoDB
  response = dynamodb.query(
        TableName= 'gen_contracted_products',
        KeyConditionExpression= 'pk = :value1 and begins_with(sk, :value2)',
        FilterExpression= '#state_private = :value3 and id_option_type = :value4',     
        ExpressionAttributeValues={
            ':value1': {'S': 'country#' + dict_aud_load_audit.get('country')},
            ':value2': {'S': 'ean_company#' + dict_path_file.get('ean_receiver')},
            ':value3': {'N': '1'},
            ':value4': {'N': '1'}
        },
        ExpressionAttributeNames={
        '#state_private': 'state'
        }
  )
  
  # verificamos que el producto contratado exista y que este activo
  if response['Items']:

    # comerciante que esta reportando la data
    trader_contrated = {'S': dict_path_file.get('ean_sender')}
    
    # verificamos si el comerciante esta contratado por el fabricante
    if trader_contrated in response['Items'][0]['ean_traders']['L']:

      # asignamos verdadeo para indicar que si esta contratado
      contrated_product = True
  
  return contrated_product



'''
Funcion que se encarga de construir el objecto detalle del archivo
para la vista previa de la pantalla de ver traza
@Parms df_sales_stock_ok dataFrame con los datos del los registros ok para ser cargados
'''
def calculate_details_sales_stock(df_sales_stock_ok):

  #Cambio el idioma a español para poder obtener el nombre del mes en español
  locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

  #copiamos el el dataFrame a uno temporal para manipularlo
  df_tmp_details = df_sales_stock_ok.copy()

  #Agregamos 0 a los valores nulos o vacios y se pasa numerico para poder sumar las cantidades
  df_tmp_details['Cantidad ventas']        = pd.to_numeric(df_tmp_details['Cantidad ventas'].fillna(0))
  df_tmp_details['Cantidad inventarios']   = pd.to_numeric(df_tmp_details['Cantidad inventarios'].fillna(0))
  df_tmp_details['Precio Unitario Venta']      = pd.to_numeric(df_tmp_details['Precio Unitario Venta'].fillna(0))
  df_tmp_details['Precio Unitario Inventario'] = pd.to_numeric(df_tmp_details['Precio Unitario Inventario'].fillna(0))

  #paso ka fecha a una fecha verdadera para poder sacar el nombre del mes de la fecha 
  df_tmp_details['tmp_date'] = pd.to_datetime(df_tmp_details.tmp_date.astype(str), format='%Y%m%d', errors='coerce')

  #agrupamos por mes y sumamos las columnas 
  df_details = df_tmp_details.groupby(df_tmp_details.tmp_date.dt.strftime('%B').str.capitalize())[['Cantidad ventas', 'Cantidad inventarios', 'Precio Unitario Venta', 'Precio Unitario Inventario']].sum()

  # regresamos el idioma al de por defecto
  locale.setlocale(locale.LC_TIME, '')

  # arreglo para almacenar los meses con sus valores
  array_details = []

  # recorremos el dataFrame con los datos del mes
  for month, data in df_details.iterrows():

    # construimos el objecto con los datos del mes
    detail_data = {
      "M": {
          "total_sales_quantity":   {"S": str(data['Cantidad ventas'])},
          "total_stock_quantity":   {"S": str(data['Cantidad inventarios'])},
          "total_sales_unit_price": {"S": str(data['Precio Unitario Venta'])},
          "total_stock_unit_price": {"S": str(data['Precio Unitario Inventario'])},
          "load_month":             {"S": month}
          }
    }
        
    # se agrega el objecto al arreglo de meses
    array_details.append(detail_data)

  #regresamos el listado de objecto
  return array_details



'''
Funcion que se encarga de calcular los valores de total, valor unitario  cuando estos vienen vacios
@Params df_sales_stock dataframe de vetas al cual se le tiene que calcular los valores vacios
@Params column_unit_price columna precio unitario
@Params column_total_price columna valor total
@params column_quatity columna cantidad
'''
def calculate_values_empty(df_sales_stock, column_unit_price, column_total_price, column_quatity):
   
  # case 1: (precio unitario != '' & valor total == ''):  precio total = cantidad * precio unitario
  df_sales_stock.loc[(((df_sales_stock[column_unit_price].notna()) &
                       (df_sales_stock[column_total_price].isna()))
                     ), column_total_price] = df_sales_stock[column_unit_price].astype(float) * df_sales_stock[column_quatity].astype(float)
  
  #case 2: ((precio unitario == '') &  valor total != '' & cantidad > 0): precio unitario = valor total / cantidad
  df_sales_stock.loc[(((df_sales_stock[column_unit_price].isna()) &
                       (df_sales_stock[column_total_price].notna()) &
                       (df_sales_stock[column_quatity].astype(float) >= 1))
                     ), column_unit_price] = df_sales_stock[column_total_price].astype(float) / df_sales_stock[column_quatity].astype(float)
  
  #case 3: ((precio unitario == '') &  valor total != '' & cantidad = 0): precio unitario = 0
  df_sales_stock.loc[(((df_sales_stock[column_unit_price].isna()) &
                       (df_sales_stock[column_total_price].notna()) &
                       (df_sales_stock[column_quatity].astype(float) == 0))
                     ), column_unit_price] = 0
  
  # se regresa el dataframe con el calculo de los valores
  return df_sales_stock


'''
Funcion que se encarga de validar que las columnas tipo texto no contengan caracteres especiales
los unicos caracteres especiales permitidos son -_/
@Params df_sales_stock_validation dataframa con el contenido del archivo cargado
'''
def validate_special_characters(df_sales_stock_validation, errors_column='errors'):
   
  # Declaramos el arreglo de columnas a validar los caracteres especiales
  array_column_special_characters = ['EAN Punto de venta', 'EAN Producto', 'Código interno', 'Código identificación tienda', 'Nombre tienda', 'Código vendedor', 'Nombre vendedor', 'Código ciudad', 'Nombre formato tienda', 'Código postal']

  # expresion regular que contiene los caracteres especiales a validar
  special_chars_pattern = re.compile(r'[^\w\s\-_/]', re.UNICODE)

  # recorremos el arreglo de columnas y vaidamos una a una que no tenga caracteres especiales en su contenido
  for column_name in array_column_special_characters:
     
    # se consulta registros que contengan una coma en el valor y agrego el mensaje de error
    df_sales_stock_validation.loc[((df_sales_stock_validation[column_name].notna()) &
                                   (df_sales_stock_validation[column_name].astype(str).str.contains(special_chars_pattern))),
                                  errors_column] += f'|El campo {column_name} no cumple con el formato definido en el mapeo de los campos'


   

import json
import boto3
import os
import awswrangler as wr
from io import StringIO
import pandas as pd
from datetime import datetime, timedelta
import dateutil
import urllib.parse
import traceback
from catch_error import catch_error
import locale
import template_mail



# Constantes
channel = 'SFTP'
document_type = 'sales_stock'
format_date = '%Y-%m-%d %H:%M:%S'
format_date_log = '%Y%m%d%H%M%S'

# Timezone
timeZone = dateutil.tz.gettz('America/Bogota')


''' Función principal '''
def lambda_handler(event, context):
    
    # Fields temporales para el procesamiento del archivo
    # Audit
    log_audit = ''
    processing_state = 'CARGA TOTAL'
    state = 'ACTIVO'
    total_records = '0'
    path_file_log = ''
    array_error = []
    details = []
    pk = ''
    sk = ''

    # Almacenar información del log de errores
    info_log = {
        'file_name_log': '',
        'path_file_log': ''
    }

    # Execution
    start_execution = None
    end_execution = None

    # Document fields
    path_file_backup = None
    dtm_report_date = None
   

    # Almacenar el tipo de error del registro
    error_type = ''
    
    
    # Se asigna fecha y hora de incio de procesamiento de la lambda
    start_execution = datetime.now(tz=timeZone).strftime(format_date)


    # Obtener la información sobre el record en formato JSON
    try:

        # Conexión con s3
        s3_client = boto3.client('s3')
        
        # SQS
        sqs_client = boto3.client("sqs")

        # Conexión con dynamodb
        dynamodb_client = boto3.client('dynamodb')
        
        event_sns = json.loads(event['Records'][0]['Sns']['Message'])['Records'][0]['s3']

        # Extraemos nombre del bucket asociado
        name_bucket = event_sns['bucket']['name']
        
        # Extraer ruta del archivo SFTP
        path_file_sftp = urllib.parse.unquote_plus(event_sns['object']['key'], encoding='utf-8')
        
        # obtener ruta de la carpeta
        path_folder = path_file_sftp.split("/IN/")[0]
        
        # Ruta de error para ubicar el archivo con los errores encontrados
        path_folder_error = path_folder + "/ERROR"
        
        # Agregar nombre del bucket al path
        path_file_sftp_bucket= f'{name_bucket}/{path_folder}/IN'
        
        # Buscamos datos en la ruta
        info_route = get_values_from_route(path_file_sftp)

        # Construimos la pk y sk para la tabla aud_load_audit
        pk = f'country#{validate_none_value(info_route["country"])}#document_type#{document_type}'
        sk = f'channel#{validate_none_value(channel)}#ean_provider#{validate_none_value(info_route["ean_receiver"])}#ean_trader#{validate_none_value(info_route["ean_sender"])}#file_name#{validate_none_value(info_route["file_name"])}'

        try:
            
            # Leemos el archivo
            df_sales_stock = wr.s3.read_csv('s3://' + name_bucket + '/' + path_file_sftp, sep=';', keep_default_na=False)

            # Modificamos los nombres de las columnas
            df_sales_stock = df_sales_stock.rename(
                columns={
                    'PUNTO DE VENTA': 'det_ean_loc',
                    'REFERENCIA SAMSUNG': 'det_ean_lin',
                    'SEMANA': 'year_week',
                    'VENTAS': 'det_qty_sale',
                    'INVENTARIO': 'det_qty_stock',
                    'CODIGO CLIENTE':'nad_ean_sender'
                }
            )
        
            total_records = str(df_sales_stock.shape[0])

            # Agregamos fechas de carga
            df_sales_stock['load_date'] = info_route['load_date']
            df_sales_stock['load_time'] =  info_route['load_time']
            df_sales_stock['dtm_report_date'] =  info_route['dtm_report_date']

            # Agregamos bgm y sendreference
            df_sales_stock['bgm_document'] = info_route['bgm']
            df_sales_stock['sendreference'] = info_route['sendreference']

            # Agregamos ean_receiver, ean_sender y country
            df_sales_stock['nad_ean_receiver'] = info_route['ean_receiver']
            df_sales_stock['nad_ean_sender'] = info_route['ean_sender']
            df_sales_stock['country'] = info_route['country']

            df_sales_stock['channel'] = channel
            df_sales_stock['document_type'] = document_type
            
            # Validaciones de estructura
            array_error = validate_structure(df_sales_stock, array_error)
            

            #Verificar si hay errores
            if len(array_error) == 0:
                # Validaciones de lógica
                # array_error, df_sales_stock_dates, error_type 
                info_validate_logitec = validate_logic(df_sales_stock, array_error, error_type)

                #Asignamos la df resultante a la df que estamos manejando globalmente
                df_sales_stock = info_validate_logitec['df_sales_stock_dates']

                # Obtenemos los información de los errores
                array_error = info_validate_logitec['array_error']
                error_type = info_validate_logitec['error_type']
                
                # Verificamos que el cliente tenga contratado el producto
                check_contracted = check_contracted_product_by_distributor(info_route, dynamodb_client)
                
                # Verificar si se encontraron errores
                if len(array_error) == 0:

                    # Creando el path del bucket backup
                    path_file_backup = 'sales_stock/'+'country=' + info_route['country'] + '/ean_provider=' + info_route['ean_receiver'] +'/ean_trader=' + info_route['ean_sender'] + '/year_load=' + info_route['load_year'] + '/month_load=' + info_route['load_month'] + '/day_load=' + info_route['load_day']
                    
                    # Validamos que el producto este contratado
                    if check_contracted:

                        # Construimos el path de salida de los archivos
                        path_sftp_sales_out = '{main_path}{file_name}_L.parquet'.format(main_path=os.environ['bucket_of_processed_file_sales'], file_name=info_route['file_name'].replace('.csv', ''))
    
                        path_sftp_stock_out = '{main_path}{file_name}_L.parquet'.format(main_path=os.environ['bucket_of_processed_file_stock'], file_name=info_route['file_name'].replace('.csv', ''))

                        # Verificamos el numero de registros que tengan inventarios y ventas vacío
                        records_empty_stock_sales = df_sales_stock[(df_sales_stock.det_qty_sale == '') & (df_sales_stock.det_qty_stock == '')].shape[0]
                        records_total_sales_stock = df_sales_stock.shape[0]
                        
                        # Verificar si todo el archivo tiene ventas e inventarios vacio
                        if (records_total_sales_stock > 0 and records_total_sales_stock == records_empty_stock_sales) :
                            processing_state='RECHAZADO POR ESTRUCTURA'
                            log_audit += "Los campos de inventario y ventas estan vacios. Por favor verifique el contenido de estas columnas"
                            array_error.append(f'{log_audit} | 0 | ')
                            error_type = 'Inventarios y ventas vacios'
                        else:
                            # Filtrar los registros con ventas e inventarios vacios
                            df_sales_stock = df_sales_stock[(df_sales_stock.det_qty_stock !='') | (df_sales_stock.det_qty_sale !='')]

                            # Reemplazar las ventas o inventartios vacios
                            replace_qty_empty_sales_stock(df_sales_stock)

                            # Terminamos de construir los parquet de cada tipo de documento, solo construimos aquellos que tengan registros
                            build_sales_parquet(df_sales_stock, path_sftp_sales_out) 
                            build_stock_parquet(df_sales_stock, path_sftp_stock_out)
                            
                            details = calculate_details(df_sales_stock)

                            # Moviendo el archivo original al bucket de backup
                            s3_client.copy_object(CopySource={'Bucket': name_bucket, 'Key': path_file_sftp}, Bucket=os.environ['backup_bucket'], Key=path_file_backup + '/' + info_route['file_name'])
                    else:
                        log_audit += '“El proveedor no tiene contratado el servicio de “Archivos Externos” con el {}'.format(info_route['name_trader'])
                        error_type = 'Servicio no contratado'
                        array_error.append(f'{log_audit} | 0 | ')
                        processing_state = 'NO CONTRATADO'
                        
                        # Moviendo el archivo original al bucket de backup
                        s3_client.copy_object(CopySource={'Bucket': name_bucket, 'Key': path_file_sftp}, Bucket=os.environ['backup_bucket'], Key=path_file_backup + '/' + file_name)
                else:
                    processing_state = 'RECHAZADO POR ESTRUCTURA'
            else:
                error_type = 'Campo(s) obligatorio(s) vacio(s)'
                # Si tenemos logs rechazamos el archivo
                processing_state = 'RECHAZADO POR ESTRUCTURA'

                

            if 'RECHAZADO POR ESTRUCTURA' == processing_state :
                # Se mueve al bucket de rechazados cuando no cumple la estructura o los segmentos obligatorios
                s3_client.copy_object(CopySource={'Bucket': name_bucket, 'Key': path_file_sftp}, Bucket=os.environ['error_processed_file'], Key='sales_stock/' + info_route['file_name'])
                
                # Modificar path_file_backup para Rechazados
                path_file_backup = f'{os.environ["error_processed_file"]}/sales_stock'
            else:     
                # Modificar path_file_backup para Backup
                path_file_backup = os.environ['backup_bucket'] + '/' + validate_none_value(path_file_backup)
            
            # Obtener la fecha y hora actual
            current_date_end = datetime.now(tz=timeZone)
            
            # Se asigna fecha y hora de fin de procesamiento de la lambda
            end_execution = current_date_end.strftime(format_date)
            
            # Se asigna fecha y hora de fin de procesamiento del log
            date_log = current_date_end.strftime(format_date_log)

            # Información para el log de errores
            info_to_log = {
                'ean_sender': info_route['ean_sender'],
                'ean_receiver': info_route['ean_receiver'],
                'date_log': date_log,
                'name_bucket': name_bucket,
                'path_folder_error': path_folder_error
            }

            # Verificar si hay errores
            if len(array_error) > 0:
              
              # Obtener información del archivo de log de errores
              info_log =  create_log_file_error(array_error, info_to_log)

              # Modificar path_file_backup para Rechazados
              path_file_backup = f'{os.environ["error_processed_file"]}/sales_stock'
            
              # Estructurar información sobre el archivo del error
              info_file_error = {
                    'path_rejected_file': f'{path_file_backup}/{info_route["file_name"]}',
                    'path_file_log': f'{info_log["path_file_log"]}/{info_log["file_name_log"]}',
                    'ean_sender': info_to_log['ean_sender'],
                    'pk': pk,
                    'sk': sk,
                    'array_error': array_error
              }

              # Enviar correo con el reporte de errores
              send_mail_report_error(info_file_error, sqs_client)

            # Estructurar información para registrar en la tabla aud_load_audit
            info_to_register = {
                'pk' : pk,
                'sk' : sk,
                'country' : info_route['country'],
                'file_name' : info_route['file_name'],
                'bgm' : info_route['bgm'],
                'sendreference' : info_route['sendreference'],
                'ean_receiver' : info_route['ean_receiver'],
                'ean_sender' : info_route['ean_sender'],
                'name_trader' : info_route['name_trader'],
                'name_provider' : info_route['name_provider'],
                'load_date' : info_route['load_date'],
                'load_time' : info_route['load_time'],
                'load_day' : info_route['load_day'],
                'load_month' : info_route['load_month'],
                'load_year' : info_route['load_year'],
                'path_file_backup' : path_file_backup,
                'path_file_sftp_bucket' : path_file_sftp_bucket,
                'dtm_report_date' : info_route['dtm_report_date'],
                'processing_state' : processing_state,
                'log_audit' : log_audit,
                'file_name_log' : info_log['file_name_log'],
                'path_file_log' : info_log['path_file_log'],
                'start_execution' : start_execution,
                'end_execution' : end_execution,
                'total_records' : total_records,
                'state' : state,
                'details' : details,
                'error_type' : error_type,
                'channel' : channel,
                'document_type': document_type
            }

            # Registramos el estado del proceso en el audit
            register_load_audit(info_to_register)

            # Eliminamos del bucket el archivo procesado
            s3_client.delete_object(Bucket=name_bucket, Key=path_file_sftp)

            return {'statusCode': 200,  'state': processing_state, 'errors': array_error, "path_file_log": path_file_log, "details": details}
        except:
            
            # Se asigna fecha y hora de fin de procesamiento de la lambda (por excepción)
            end_execution = datetime.now(tz=timeZone).strftime(format_date)

            processing_state = 'RECHAZADO POR ESTRUCTURA'

            # Se obtiene el error
            #print(traceback.format_exc())
            log_audit='Ha ocurrido un error al procesar el archivo. Por favor verifique que la extension del archivo sea ".csv", que el separador de columnas sea punto y coma " ; " y que el formato y la estructura sea correcta.'
            error_type = 'Estructura, extensión, separador incorrecto'
            array_error.append(f'{log_audit} | 0 | ')
            
            # Se asigna fecha y hora de fin de procesamiento del log
            date_log = f'{info_route["load_date"]}{info_route["load_time"]}'

             # Información para el log de errores
            info_to_log = {
                'ean_sender': info_route['ean_sender'],
                'ean_receiver': info_route['ean_receiver'],
                'date_log': date_log,
                'name_bucket': name_bucket,
                'path_folder_error': path_folder_error
            }

            #Creando el archivo de log de errores
            info_log = create_log_file_error(array_error, info_to_log)

            # Modificar path_file_backup para Rechazados
            path_file_backup = f'{os.environ["error_processed_file"]}/sales_stock'

            # Estructurar información para registrar en la tabla aud_load_audit
            info_to_register = {
                'pk' : pk,
                'sk' : sk,
                'country' : info_route['country'],
                'file_name' : info_route['file_name'],
                'bgm' : info_route['bgm'],
                'sendreference' : info_route['sendreference'],
                'ean_receiver' : info_route['ean_receiver'],
                'ean_sender' : info_route['ean_sender'],
                'name_trader' : info_route['name_trader'],
                'name_provider' : info_route['name_provider'],
                'load_date' : info_route['load_date'],
                'load_time' : info_route['load_time'],
                'load_day' : info_route['load_day'],
                'load_month' : info_route['load_month'],
                'load_year' : info_route['load_year'],
                'path_file_backup' : path_file_backup,
                'path_file_sftp_bucket' : path_file_sftp_bucket,
                'dtm_report_date' : info_route['dtm_report_date'],
                'processing_state' : processing_state,
                'log_audit' : log_audit,
                'file_name_log' : info_log['file_name_log'],
                'path_file_log' : info_log['path_file_log'],
                'start_execution' : start_execution,
                'end_execution' : end_execution,
                'total_records' : total_records,
                'state' : state,
                'details' : details,
                'error_type' : error_type,
                'channel' : channel,
                'document_type': document_type
            }

            # Registrando archivos corruptos en la tabla aud_load_audit de dynamodb
            register_load_audit(info_to_register)
            
            # Se mueve al bucket de rechazados cuando no cumple la estructura o los segmentos obligatorios
            s3_client.copy_object(CopySource={'Bucket': name_bucket, 'Key': path_file_sftp}, Bucket=os.environ['error_processed_file'], Key='sales_stock/' + info_route['file_name'])

            # Estructurar información sobre el archivo del error
            info_file_error = {
                    'path_rejected_file': f'{path_file_backup}/{info_route["file_name"]}',
                    'path_file_log': f'{info_log["path_file_log"]}/{info_log["file_name_log"]}',
                    'ean_sender': info_route['ean_sender'],
                    'pk': pk,
                    'sk': sk,
                    'array_error': array_error
            }
            
            # Enviar correo con el reporte de errores
            send_mail_report_error(info_file_error, sqs_client)
            
            # Removiendo el archivo CSV procesado del bucket SFTP
            s3_client.delete_object(Bucket=name_bucket, Key=path_file_sftp)
            
            return {'statusCode': 500,  'state': processing_state, 'errors': array_error, "log_error": log_audit}

    except:

        '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
        e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
        catch_error(traceback, context, dateutil, datetime, boto3)
        raise



'''Función que crea el archivo de log de errores'''
def create_log_file_error(array_error, info_to_log):
    # Crear un DataFrame de pandas
    df_erros = pd.DataFrame({'DESCRIPCIONERROR | NUMEROLINEA | VALORLINEA ': array_error})
    
    # nombre del archivo del log de errores
    file_name_log = f'{info_to_log["ean_sender"]}_{info_to_log["ean_receiver"]}_{info_to_log["date_log"]}_log.csv'
    
    # Ubicación archivo log de errores para el log
    path_file_log = f'{info_to_log["name_bucket"]}/{info_to_log["path_folder_error"]}'
    
    #Ubicación del archivo
    path= f'{path_file_log}/{file_name_log}'

    wr.s3.to_csv(df = df_erros, path = 's3://'  + path, header=True, index=False)

    return {'file_name_log': file_name_log, 'path_file_log': path_file_log}



'''Función que valida que el documento tenga la estructura base adecuada para su procesamiento'''
def validate_structure(df_sales_stock, array_error):

    # VALIDACION 1: REFERENCIA SAMSUNG
    # Obtener los indices de los registros que tengan el campo REFERENCIA SAMSUNG vacio
    array_index_empty_product = df_sales_stock[df_sales_stock.det_ean_lin == ''].index.tolist()
    
    # Validar que se hayan obtenido indices de registros con la referencia vacio
    if len(array_index_empty_product) > 0:
        array_error = format_log(array_index_empty_product,df_sales_stock,'El campo REFERENCIA SAMSUNG no puede estar vacío.',array_error)
    
    # VALIDACION 2: Punto de venta obligatorio
    # Obtener los indices de los registros que tengan el campo PUNTO DE VENTA vacio
    array_index_empty_point = df_sales_stock[df_sales_stock.det_ean_loc == ''].index.tolist()
    
    # Validar que se hayan obtenido indices de registros con la punto de venta vacio
    if len(array_index_empty_point) > 0:
        array_error =format_log(array_index_empty_point,df_sales_stock,'El campo PUNTO DE VENTA no puede estar vacío.',array_error)
     
    # VALIDACION 3: semama campo obligatorio
    # Obtener los indices de los registros que tengan el campo FECHA vacio
    array_index_empty_date = df_sales_stock[df_sales_stock.year_week == ''].index.tolist()
    
    # Validar que se hayan obtenido indices de registros con la punto de venta vacio
    if len(array_index_empty_date) > 0:
        array_error = format_log(array_index_empty_date,df_sales_stock,'El campo SEMANA no puede estar vacío.',array_error)
        
    return array_error



''' Estructura Log de error '''
def format_log(array_duplicated_index, df_sales_stock, msg, array_error):
    for index in array_duplicated_index:
        value_row = df_sales_stock.loc[index]
        value_csv = ';'.join(map(str, value_row))
        array_error.append(f" {msg} | {index + 1} | {value_csv}")
    return array_error



'''Función que valida aspectos de lógica de negocio sobre el documento entrante'''
def validate_logic(df_sales_stock, array_error, error_type):
    
    # Almacenar si obtiene un log de error por duplicado
    array_error = validate_duplicated(df_sales_stock, array_error)
    
    # Verificar que no se haya generado un log de error
    if len(array_error) == 0:
        # Sacamos las semanas en el documento
        array_weeks_on_document = df_sales_stock.loc[df_sales_stock.year_week != '', 'year_week'].unique()
        
        # Declaramos df de dates
        df_dates = pd.DataFrame(columns=('year_week','tmp_date'))
    
    
        # VALIDACION 2: Verificamos que las semanas en el documento estén en el plazo establecido
        for index, week in enumerate(array_weeks_on_document):
            str_week=str(week)

            value_row = df_sales_stock.loc[index]
            value_csv = ';'.join(map(str, value_row))

            if len(str_week) == 6:
                
                #Agregamos la fecha actual a df dates
                df_current_date = pd.DataFrame({ 'year_week': [week], 'tmp_date': [get_last_day_of_week(str_week)] })
                df_dates = pd.concat([df_dates,df_current_date], ignore_index=True)
    
                # Validamos el deadline
                is_validate_deadline = validate_deadline_by_iso_week(str_week)
    
                if is_validate_deadline == False:
                    array_error.append(f" La semana reportada {str_week} no cumple con los plazos establecidos para el procesamiento del archivo |{index + 1}|{value_csv}")
                    error_type = 'Fecha no cumple con el plazo establecido'
                    
            else:
                array_error.append(f"El campo SEMANA {str_week} no contiene el formato correcto |{index + 1}|{value_csv}")
                error_type = 'Fecha con formato incorrecto'
        
        # Hacemos merge de los dos dataframe para incluir las fechas 
        df_sales_stock_dates = df_sales_stock.merge(df_dates, on='year_week', how = 'inner')
        
        return {'array_error': array_error, 'df_sales_stock_dates': df_sales_stock_dates, 'error_type':error_type}
    else:
        error_type = 'Registros duplicados'

    return {
        'array_error': array_error, 'df_sales_stock_dates': df_sales_stock, 'error_type':error_type
    }    

    
''' Validar duplicados '''    
def validate_duplicated(df_sales_stock, array_error):
    #Validar duplicidad de registros
    # Realizar la agregación con groupby y contar
    df_duplicated = df_sales_stock[df_sales_stock.duplicated(subset=['det_ean_loc','det_ean_lin', 'year_week'], keep=False)].reset_index()
    
    # Obtener los indicices de los registros repetidos
    array_duplicated_index = df_duplicated['index'].values
    
    # validar si hay registros duplicados
    if not df_duplicated.empty:
        array_error = format_log(array_duplicated_index,df_sales_stock,'El registro se encuentra duplicado en los campos PUNTO DE VENTA, REFERENCIA SAMSUNG, SEMANA',array_error)
         
    return array_error



'''Valida que el archivo se encuentre en el plazo estipulado de semanas para procesamiento'''
def validate_deadline_by_iso_week(year_week):

    # Obtén la fecha y hora actual
    current_date_iso = datetime.today().isocalendar()

    # Hallo la fecha del primer día de la semana
    current_date = datetime.fromisocalendar(current_date_iso[0], current_date_iso[1], 1)

    # Tomo la fecha del archivo basado en su semana ISO y calculo la fecha del primer día de esa semana
    year_iso = int(year_week[:4])
    week_iso = int(year_week[-2:])

    file_date = datetime.fromisocalendar(year_iso, week_iso, 1)

    # Resto la fecha del primer dia de la semana iso actual con la fecha del archivo
    weeks_difference = (current_date - file_date).days / 7
    
    # Si es más de 4 semanas  entre la fecha del archivo y y la fecha actual la rechaza
    if weeks_difference > 4:
        return False
    else:
        # Si es menor está dentro del plazo y procesa
        return True
        

'''Reemplazar ventas o inventarios vacíos'''
def replace_qty_empty_sales_stock(df_sales_stock):
    df_sales_stock.loc[(df_sales_stock.det_qty_sale != '') & (df_sales_stock.det_qty_stock == ''), 'det_qty_stock'] = '0'
    df_sales_stock.loc[(df_sales_stock.det_qty_stock != '') & (df_sales_stock.det_qty_sale == ''), 'det_qty_sale'] = '0'


'''Función que consulta si el distribuidor tiene el producto contratado'''
def check_contracted_product_by_distributor(info_route, dynamodb_client):
    
    # Construyo la pk para la tabla gen_contracted_products
    pk = 'country#' + info_route['country']
    query = '''select name_company from gen_contracted_products WHERE pk = '{0}' and ean_company = '{1}' and id_option_type = 1 and contains(ean_traders,'{2}')'''.format(pk, info_route['ean_receiver'], info_route['ean_sender'])

    # Se ejecuta el query sobre dynamodb
    response_query = dynamodb_client.execute_statement(Statement=query)
    response_query = response_query['Items']

    # Si tenemos respuesta significa que tiene el producto contratado
    if len(response_query) > 0:
        return True
    else:
        return False


'''
Funcion que se encarga de consultar el nombre de la compañias 
@Params type_company typo de compañia a consultar
@Params ean_company Ean de la compañia a consultar
'''
def get_company_name(type_company, ean_company):
    
    # Conexión con dynamodb
    dynamodb_client = boto3.client('dynamodb')
            
    #realizamos la consulta hacia dynamoDB
    response = dynamodb_client.query(
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


''' Función que permite extraer algunoss campos de la ruta del archivo '''
def get_values_from_route(bucket_route):

    # Se obtiene fecha y hora actual del sistema
    current_date = datetime.now(tz=timeZone)
    
    lst_route = bucket_route.split('/')
    
    country_ean_receiver = lst_route[6]

    # Obtener ean de las compañías
    ean_receiver = country_ean_receiver.split('_')[1]
    ean_sender = lst_route[5]   
    
    # consultamos las empresas fabricantes y comerciantes
    company_sender = get_company_name('C', ean_sender)
    company_receiver = get_company_name('F', ean_receiver) 
    
    # Obtener los nombres de las empresas
    name_trader   = company_sender.get('company_name').get('S')
    name_provider  = company_receiver.get('company_name').get('S')

    country = country_ean_receiver.split('_')[0]
    file_name = lst_route[8]
    
    '''
    Variables que me permiten definir la fecha de carga, cuando el archivo es cargado por primera vez se toma la fecha del sistema
    pero cuando el archivo es recargado entonces toma la fecha de carga que esta en el nombre del archivo como tal
    '''
    try:
        load_date_file = file_name.split('_CO.')[0][-14:]

        datetime.strptime(load_date_file, '%Y%m%d%H%M%S') # Formato de fecha esperado

        load_date = dtm_report_date = load_date_file[:8]      # Toma los primeros 8 caracteres que representan yyyymmdd
        load_time = load_date_file[-6:]     # Toma los ultimos 6 caracteres que representan hhmiss
        load_year = load_date_file[:4]      # Toma los primeros 4 caracteres que representan yyyy
        load_month = load_date_file[4:6]    # Toma los caracteres desde la posición 4 a la 6 que representan mm
        load_day = load_date_file[6:8]      # Toma los caracteres desde la posición 6 a la 8 que representan dd
        bgm = sendreference = current_date.strftime('%Y%m%d%H%M%S%f')

    except ValueError:
        
        load_date = current_date.strftime('%Y%m%d')
        load_time = current_date.strftime('%H%M%S')
        load_year = str(current_date.year)
        load_month = str(current_date.month).zfill(2)
        load_day = str(current_date.day).zfill(2)
        dtm_report_date = current_date.strftime('%Y%m%d')
        bgm = sendreference = current_date.strftime('%Y%m%d%H%M%S%f')
        

        if '.csv' in file_name or '.CSV' in file_name:
            file_name =file_name.replace('.csv',  '_' + load_date + load_time + '_' + country + '.csv').replace('.CSV',  '_' + load_date + load_time + '_' + country + '.CSV')
        elif '.xlsx' in file_name or '.XLSX' in file_name:
            file_name =file_name.replace('.xlsx',  '_' + load_date + load_time + '_' + country + '.xlsx').replace('.XLSX',  '_' + load_date + load_time + '_' + country + '.XLSX')
        elif '.txt' in file_name or '.TXT' in file_name:
            file_name =file_name.replace('.txt',  '_' + load_date + load_time + '_' + country + '.txt').replace('.TXT',  '_' + load_date + load_time + '_' + country + '.TXT')
    
    return {'ean_sender': ean_sender, 'ean_receiver': ean_receiver, 'country': country, 'file_name': file_name, 'name_provider': name_provider, 'name_trader': name_trader, 'load_date': load_date, 'load_time': load_time, 'load_year': load_year, 'load_month': load_month, 'load_day': load_day, 'dtm_report_date': dtm_report_date, 'bgm': bgm, 'sendreference': sendreference}



''' Función para obtener el último día de la semana '''
def get_last_day_of_week(year_week):

    year_iso = int(year_week[:4])
    week_iso = int(year_week[-2:])

    # Obtener el primer día de la semana (lunes) usando isocalendar
    first_day_of_week = datetime.fromisocalendar(year_iso, week_iso, 1)

    # Calcular el último día de la semana sumando 6 días al primer día de la semana
    last_day_of_week = first_day_of_week + timedelta(days=6)

    return last_day_of_week.strftime('%Y%m%d')



''' Función que permite guardar auditoria de los EDI recibidos por parte del cliente '''
def register_load_audit(info_to_register):
    
    # Conexión con dynamodb
    dynamodb_client = boto3.client('dynamodb')

    array_file_name = info_to_register['file_name'].split(".")
  
    extension_file = array_file_name[len(array_file_name) - 1]
    
    info_to_register['file_name'] = info_to_register['file_name'].replace('.' + extension_file, '')

    dynamodb_client.put_item(
        TableName='aud_load_audit',
        Item={
            'pk': {'S': info_to_register['pk']},
            'sk': {'S': info_to_register['sk']},
            'bgm': {'S': validate_none_value(info_to_register['bgm'])},
            'country': {'S': validate_none_value(info_to_register['country'])},
            'ean_provider': {'S': validate_none_value(info_to_register['ean_receiver'])},
            'ean_trader': {'S': validate_none_value(info_to_register['ean_sender'])},
            'file_name': {'S': validate_none_value(info_to_register['file_name'])},
            'load_date': {'S': validate_none_value(info_to_register['load_date'])},
            'load_day': {'S': validate_none_value(info_to_register['load_day'])},
            'load_hour': {'S': validate_none_value(info_to_register['load_time'])},
            'load_month': {'S': validate_none_value(info_to_register['load_month'])},
            'load_year': {'S': validate_none_value(info_to_register['load_year'])},
            'path_file': {'S': info_to_register['path_file_backup']},
            'provider': {'S': validate_none_value(info_to_register['name_provider'])},
            'reported_end_date': {'S': validate_none_value(info_to_register['dtm_report_date'])},
            'reported_start_date': {'S': validate_none_value(info_to_register['dtm_report_date'])},
            'snrf': {'S': validate_none_value(info_to_register['sendreference'])},
            'state': {'S': info_to_register['state']},
            'trader': {'S': validate_none_value(info_to_register['name_trader'])},
            'document_type': {'S': info_to_register['document_type']},
            'processing_state': {'S': info_to_register['processing_state']},
            'log_audit': {'S': validate_none_value(info_to_register['log_audit'])},
            'start_execution': {'S': validate_none_value(info_to_register['start_execution'])},
            'end_execution': {'S': validate_none_value(info_to_register['end_execution'])},
            'total_records': {'S': info_to_register['total_records']},
            'file_name_log': {'S': info_to_register['file_name_log']},
            'path_file_log': {'S': info_to_register['path_file_log']},
            'path_file_sftp': {'S': info_to_register['path_file_sftp_bucket']},
            'channel': {'S': info_to_register['channel']},
            'email_load': {'S': 'soporteprescriptiva@gmail.com'},
            'user_load': {'S': 'soporte'},
            'extension_file': {'S': extension_file},
            'details': {'L': info_to_register['details']},
            'sk_lsi1': {'S': validate_none_value(info_to_register['file_name'])},
            'error_type': {'S': info_to_register['error_type']}
        },
        ReturnValues='NONE',
    )


# Funciones de parquetización

'''Función que construye y guarda el parquet del tipo de archivo Ventas'''
def build_sales_parquet(df_sales, path_sftp_sales_out):
    
    #Filtramos la df para que tenga solo registros con valores de sales
    df_sales_parquet = df_sales[df_sales.det_qty_sale != ''].copy()
    
    # Modificamos los nombres de las columnas
    df_sales_parquet.rename(columns ={'tmp_date' : 'dtm_sale_date'}, inplace = True)
    
    # Asignamos el valor de path_of_processed_file
    df_sales_parquet['path_of_processed_file'] = path_sftp_sales_out

    # Borramos columnas del dataframe
    df_sales_parquet.drop('det_qty_stock', axis=1, inplace = True)
    df_sales_parquet.drop('year_week', axis=1, inplace = True)
  
    # Campos adiconales para complementar el parquete de ventas
    df_sales_parquet['det_pri_neto'] = None
    df_sales_parquet['det_pri_neto_total'] = None

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

    #print("************** PRINT DATAFRAME ORIGINAL SALES *************")
    #print(df_sales_parquet.to_string(max_rows=None, max_cols=None))

    df_sales_parquet = df_sales_parquet[['load_date', 'load_time', 'bgm_document', 'country', 'nad_ean_receiver', 'nad_ean_sender', 'dtm_report_date', 'path_of_processed_file', 'det_ean_lin', 'det_qty_sale', 'det_ean_loc', 'det_pri_neto', 'det_pri_neto_total', 'dtm_sale_date', 'sendreference', 'channel', 'document_type']]

    # ********** Generate Parquet file based on data list **********
    wr.s3.to_parquet(dtype=sales_dtypes, df=df_sales_parquet, path=path_sftp_sales_out)



'''Función que construye y guarda el parquet del tipo de archivo Inventarios'''
def build_stock_parquet(df_stock, path_sftp_stock_out):

    #Filtramos la df para que tenga solo registros con valores de stock
    df_stock_parquet = df_stock[df_stock.det_qty_stock != ''].copy()
    
    # Modificamos los nombres de las columnas
    df_stock_parquet.rename( columns = { 'tmp_date' : 'dtm_stock_initial_date' }, inplace = True )
    df_stock_parquet['dtm_stock_final_date'] = df_stock_parquet['dtm_stock_initial_date']

    # Asignamos el valor de path_of_processed_file
    df_stock_parquet['path_of_processed_file'] = path_sftp_stock_out
    
    # Borramos columnas del dataframe
    df_stock_parquet.drop('det_qty_sale', axis=1, inplace = True)
    df_stock_parquet.drop('year_week', axis=1, inplace = True)
    
    # Campos adiconales para complementar el parquete de inventarios
    df_stock_parquet['det_pri_neto'] = None
    df_stock_parquet['det_pri_list'] = None
    df_stock_parquet['det_qty_in_transit'] = None
    df_stock_parquet['det_ean_lin_upc'] = None
    df_stock_parquet['det_pia_in'] = None
    df_stock_parquet['det_pia_sa'] = None
    df_stock_parquet['det_imd'] = None
    df_stock_parquet['det_ali'] = None


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
    #print("************** PRINT DATAFRAME ORIGINAL STOCK *************")
    #print(df_stock_parquet.to_string(max_rows=None, max_cols=None))

    df_stock_parquet = df_stock_parquet[['bgm_document', 'nad_ean_sender', 'nad_ean_receiver', 'dtm_report_date', 'dtm_stock_initial_date', 'dtm_stock_final_date', 'country', 'load_date', 'load_time', 'det_ean_lin', 'det_ean_lin_upc', 'det_pia_in', 'det_pia_sa', 'det_imd', 'det_ali', 'det_qty_stock', 'det_qty_in_transit', 'det_ean_loc', 'det_pri_neto', 'det_pri_list','path_of_processed_file','sendreference', 'channel', 'document_type']]
    
    # ********** Generando archivo parquet **********
    wr.s3.to_parquet(dtype=stock_dtypes, df=df_stock_parquet, path=path_sftp_stock_out)



''' Calcular detalles mensuales '''
def calculate_details(df_sales_stock):
    # Establecer el idioma local a español
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
    
    df = df_sales_stock.copy()
    
    # Convertir las columnas 'det_qty_sale' y 'det_qty_stock' a números
    df['det_qty_sale'] = pd.to_numeric(df['det_qty_sale'], errors='coerce')
    df['det_qty_stock'] = pd.to_numeric(df['det_qty_stock'], errors='coerce')
    
    
    # Convertir la columna 'year_week' a tipo datetime sin zona horaria
    df['year_week'] = pd.to_datetime(df['year_week'].astype(str) + '0', format='%Y%W%w', errors='coerce')
    
    # Agrupar por mes y calcular sumas
    df_details_data = df.groupby(df.year_week.dt.strftime('%B').str.capitalize())[['det_qty_sale', 'det_qty_stock']].sum()
    
    # Restaurar el idioma local a su valor predeterminado
    locale.setlocale(locale.LC_TIME, '')
    
    # Almacenar resumen de los meses
    array_details_data = []

    # Crear el formato de salida
    for month, value in df_details_data.iterrows():

        # Total cantidad de ventas del mes
        total_sales_quantity = value['det_qty_sale']
        
        # Total cantidad de inventario del mes
        total_stock_quantity = value['det_qty_stock']
        
        # Resumen del mes
        detail_data = {
            "M": {
                "total_sales_quantity": {"S": str(total_sales_quantity)},
                "total_stock_quantity": {"S": str(total_stock_quantity)},
                "load_month": {"S": month}
            }
        }
        
        # Agregar el resumen del mes
        array_details_data.append(detail_data)
        
    return array_details_data



'''
Esta función permite validar si el valor del parámetro "value" tiene el valor "None" y devuelve '' o "value" en caso contrario.
'''
def validate_none_value(value):
    return value if value is not None else ''


'''Función para enviar el reporte sobre el archivo rechazado '''
def send_mail_report_error(info_file_error, sqs_client):
    
    #Obtenemos el archivo excel del bucket
    df_emails_commerces = wr.s3.read_csv(os.environ['file_emails'] + '/emails/emails_commerces_samsung.csv', sep=';', keep_default_na=False)
    
     # Modificamos los nombres de las columnas
    df_emails_commerces = df_emails_commerces.rename(
        columns={
            'Número de identificación': 'id_number_commerce',
            'Nombre Empresa': 'commerce_name',
            'Correo electrónico Samsung': 'email_electronic_samsung',
            'Correo Samsung ': 'email_samsung',
            'Correo electrónico contacto Distribuidor': 'email_contact_distributor',
            'Correo electrónico contacto samsung': 'email_contact_samsung',
        }
    )
    
    # Obtener información de los errores
    info_message = template_mail.structure_erros_mail(info_file_error['array_error'])
    
    # Obtener la información relacionada con el comercio
    filter_row = df_emails_commerces[df_emails_commerces.id_number_commerce == int(info_file_error['ean_sender'])]

    # Verificar que se haya encontrado información sobre el comercio 
    if not filter_row.empty:
        # Obtener el nombre del comercio
        commerce_name = filter_row.commerce_name.iloc[0]

        # Estructurar el cuerpo del mensaje del correo
        info_message['commerce_name'] = commerce_name

        # Estructurar el cuerpo del mensaje del correo
        text_body = template_mail.structure_text_body(info_message)
        
        # Obtener los correos electrónicos
        emails_prescriptiva = "[juan.neira@carvajal.com, luis.cortesr@carvajal.com, manuel.millan@carvajal.com" 
        email_distributor = filter_row.email_contact_distributor.iloc[0].replace(";", ", ")
        email_elctronic_samsung = filter_row.email_electronic_samsung.iloc[0].replace(";", ", ")
        email_samsung = filter_row.email_samsung.iloc[0].replace(";", ", ")
        email_contact_samsung = filter_row.email_contact_samsung.iloc[0].replace(";", ", ")
        
         # Estructurar los correos electrónicos para enviar
        emails_to_cc = f'{emails_prescriptiva}, {email_elctronic_samsung}, {email_samsung}, {email_contact_samsung}]'
        email_to = f'[{email_distributor}]'
        
        # Estructurar el información para enviar a la cola
        dict_info_send_mail = {
            "to": email_to,
            "subject": 'DOCUMENTOS RECHAZADOS - ERROR DE ESTRUCTURA',
            "cc": emails_to_cc,
            "body": text_body,
            "attachment": f"[{info_file_error['path_file_log']}, {info_file_error['path_rejected_file']}]",
            "data": {
                "table": "aud_load_audit",
                "pk": info_file_error['pk'],
                "sk": info_file_error['sk']
            }
        }
        
        # Convertir el diccionario a un json
        dict_info_send_mail = json.dumps(dict_info_send_mail)
        print("email ", dict_info_send_mail)
        
        # Enviar información a la cola
        sqs_client.send_message(
                                    QueueUrl=os.environ['sqs_send_mail'],
                                    MessageBody=dict_info_send_mail
                                )
        
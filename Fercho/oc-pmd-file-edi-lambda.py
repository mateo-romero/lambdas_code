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
document_type = 'oc_pmd'

'''Función princial de la lambda'''
def lambda_handler(event, context):
  
  try:
    
      # Estableciendo conexión a dynamodb
      dynamodb_client = boto3.client("dynamodb")
    
      # Extraer información del documento que llegó al bucket. En este caso extrae el nombre del bucket.
      name_bucket = event['Records'][0]['s3']['bucket']['name']
      
      path_file = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
      
      # Se hace split por el separador "_"
      path_file_split_by_first_delimiter = path_file[:-4].split("_")
      
      # Se hace split por el separador "/"
      path_file_split_by_second_delimiter = path_file.split("/")
      
      # Se obtiene el país a partir del nombre del archvio.
      country = path_file_split_by_first_delimiter[len(path_file_split_by_first_delimiter)-1]
      
      full_date = path_file_split_by_first_delimiter[len(path_file_split_by_first_delimiter)-2]
      
      # Se obtiene el nombre del archivo
      file_name = path_file_split_by_second_delimiter[len(path_file_split_by_second_delimiter)-1]
      
      id_orden = path_file_split_by_first_delimiter[len(path_file_split_by_first_delimiter)-4]
      
      id_empresa = path_file_split_by_first_delimiter[len(path_file_split_by_first_delimiter)-3]
      
      # Se obtiene la fecha y hora a partir del campo full_date
      load_date = full_date[:8]       # [LOAD_DATE]
      load_time = full_date[8:]      # [LOAD_TIME]
      
      # Split load_date into different variables to get the load_year, load_month and load_day.
      load_year = load_date[:4]
      load_month = load_date[-4:-2]
      load_day = load_date[-2:]
      
      # Temporary variables to store data from .EDI document
      header_data = []
      detail_data = [] # NO APLICA
      data = []
      
      # Temporary Fields referenced by header. Initializing in None
      head_unb_sender = None
      head_unb_receiver = None
      head_nad_by = None
      head_nad_su = None
      head_nad_dp = None
      head_bgm_message_type = None
      head_bgm_document_id = None
      head_dtm_document_date = None
      head_dtm_minimum_date = None
      head_dtm_maximum_date = None
      head_loc_place_identificacion = None
      
      # Campos temporales para el detalle inicializados en NONE
      det_ean_lin = None
      det_pri_aaa = None
      det_pri_aab = None
      det_pia_sa = None
      det_tax_iva = None
      det_tax_ipoconsumo = None
      det_requested_quantity = None           # CALIFICADOR 21
      det_requested_qty_measure_unit = None   # CALIFICADOR 21
      det_requested_qty_type = None           # CALIFICADOR 21
      det_bonus_quantity = None               # CALIFICADOR 192
      det_bonus_qty_measure_unit = None       # CALIFICADOR 192
      det_bonus_qty_type = None               # CALIFICADOR 192
      det_loc_lin = None
      det_consecutive_lin = 0
      
      # Temporary Fields for UNZ Segment
      unz_snrf = None
      
      # Variables to use in log audit
      log_audit = None
      processing_state = 'CARGA TOTAL'
      state = 'ACTIVO'
      total_records = 0
      start_execution = None
      end_execution = None
      provider = None
      trader = None
      prefix = None
      
      # Variables to check if First LIN Segment
      IS_FIRST_LIN = True
      
      # Temporary variables to store data from .EDI document
      header_data = []
      detail_data = [] # NO APLICA
      data = []
      
      # Variables temporales para crear las estructuras desagregadas requeridas.
      purchase_order_status_data = []
      purchase_order_data = []
      items_data = []
      items_quantity_data = []
      
      # Leer el contenido del documento
      object_with_document = bucket_connection.get_object(Bucket=name_bucket, Key=path_file)
      document_content = object_with_document['Body'].read()
      
      timeZone = dateutil.tz.gettz('America/Bogota')
      format_date = '%Y-%m-%d %H:%M:%S'
      
      full_date = datetime.strptime(full_date, '%Y%m%d%H%M').strftime('%Y-%m-%d %H:%M:%S')
      
      # Se asigna fecha y hora de incio de procesamiento de la lambda
      start_execution =  datetime.now(tz=timeZone).strftime(format_date)
      
      try:
  
        # Decodifica el contenido del archivo que tenía formato UTF8
        document_content_decode = document_content.decode('UTF-8')
        
        # Segmenta el documento edit por etiquetas, de esta forma podrás acceder a las variables de una manera fácil
        edi_file = RawSegmentCollection.from_str(document_content_decode)
        
        # Se define esta variable para controlar la cantidad de UNH
        CUR_UNH = -1
      
        # Se itera los segmentos del EDI
        for segment in edi_file.segments:
          
          if (segment.tag == 'UNB'):
            head_unb_sender = segment.elements[1]
            head_unb_receiver_tmp = segment.elements[2]
            
            if isinstance(head_unb_receiver_tmp, list):
              head_unb_receiver = head_unb_receiver_tmp[0]
            else:
              head_unb_receiver = head_unb_receiver_tmp
            
          elif segment.tag == 'UNH':
            CUR_UNH += 1
            
            # Bucket donde quedan transformados los EDI en formato parquet
            path_of_processed_file_1 = os.environ['bucket_of_processed_file']+file_name[:-4]+'_CONSEC_'+str(CUR_UNH)+'_L_1.csv'
            path_of_processed_file_2 = os.environ['bucket_of_processed_file']+file_name[:-4]+'_CONSEC_'+str(CUR_UNH)+'_L_2.csv'
            path_of_processed_file_3 = os.environ['bucket_of_processed_file']+file_name[:-4]+'_CONSEC_'+str(CUR_UNH)+'_L_3.csv'
            path_of_processed_file_4 = os.environ['bucket_of_processed_file']+file_name[:-4]+'_CONSEC_'+str(CUR_UNH)+'_L_4.csv'
              
            if CUR_UNH > 0:
              
              items_data.append([str(id_orden)+'_'+str(CUR_UNH), 
                                id_empresa, 
                                det_consecutive_lin, 
                                det_ean_lin, 
                                det_pia_sa, 
                                det_tax_iva, 
                                det_tax_ipoconsumo, 
                                det_pri_aaa, 
                                det_pri_aab, 
                                load_date, 
                                country,
                                'ITEMS_OC', 
                                '', 
                                '',
                                channel,
                                document_type])
    
              if det_requested_qty_type == '21':
                items_quantity_data.append([str(id_orden)+'_'+str(CUR_UNH), 
                                            id_empresa, 
                                            det_consecutive_lin, 
                                            det_requested_qty_type, 
                                            det_requested_quantity, 
                                            det_requested_qty_measure_unit, 
                                            load_date, 
                                            country,
                                            'ITEMS_QUANTITY_OC',
                                            channel,
                                            document_type])
    
              if det_bonus_qty_type == '192':
                items_quantity_data.append([str(id_orden)+'_'+str(CUR_UNH), 
                                            id_empresa, 
                                            det_consecutive_lin, 
                                            det_bonus_qty_type, 
                                            det_bonus_quantity, 
                                            det_bonus_qty_measure_unit, 
                                            load_date, country,
                                            'ITEMS_QUANTITY_OC',
                                            channel,
                                            document_type])
    
              purchase_order_status_data.append([str(id_orden)+'_'+str(CUR_UNH), 
                                                id_empresa, 
                                                head_unb_sender, 
                                                head_unb_receiver, 
                                                load_date, 
                                                country, 
                                                'ESTADOS_OC',
                                                channel,
                                                document_type])
    
              purchase_order_data.append([str(id_orden)+'_'+str(CUR_UNH), 
                                          id_empresa, 
                                          head_nad_by, 
                                          head_nad_su, 
                                          head_nad_dp, 
                                          head_bgm_message_type, 
                                          head_bgm_document_id, 
                                          head_dtm_document_date, 
                                          head_dtm_minimum_date, 
                                          head_dtm_maximum_date, 
                                          head_loc_place_identificacion, 
                                          load_date, 
                                          country,
                                          'OC',
                                          channel,
                                          document_type])
              
              # Dataframe de Estados Orden de Compra
              df_1 = pd.DataFrame(purchase_order_status_data, 
                                  columns=['id_orden',
                                          'id_empresa', 
                                          'ean_buzon_emisor', 
                                          'ean_buzon_receptor', 
                                          'fcha_actlzcn_regstr', 
                                          'pais',
                                          'file_type',
                                          'channel',
                                          'document_type'])
        
              # Guarda el DataFrame en un archivo CSV en la ruta definida en las variables de entorno
              wr.s3.to_csv(df_1, path=path_of_processed_file_1, index=False, sep=';')
              
              # Dataframe de Orden de Compra
              df_2 = pd.DataFrame(purchase_order_data, 
                                  columns=['id_orden',
                                          'id_empresa',
                                          'nad_by_comprador',
                                          'nad_su_entidad_proveedora',
                                          'nad_dp_lugar_entrega',
                                          'id_bgm_tipo_mensaje',
                                          'id_bgm_documento',
                                          'dtm_fecha_documento',
                                          'fecha_minima',
                                          'fecha_maxima',
                                          'loc_identificacion_lugar', 
                                          'fcha_actlzcn_regstr', 
                                          'pais',
                                          'file_type',
                                          'channel',
                                          'document_type'])
              
              # Guarda el DataFrame en un archivo CSV en la ruta definida en las variables de entorno
              wr.s3.to_csv(df_2, path=path_of_processed_file_2, index=False, sep=';')
                
              # Dataframe de Items OC
              df_3 = pd.DataFrame(items_data,
                                  columns=['id_orden',
                                          'id_empresa',
                                          'consecutivo',
                                          'ean_articulo_lin',
                                          'pia_sa_codigo_empresa',
                                          'tax_iva',
                                          'tax_ipoconsumo',
                                          'pri_precio_bruto',
                                          'pri_precio_neto', 
                                          'fcha_actlzcn_regstr', 
                                          'pais',
                                          'file_type',
                                          'moa_iva_cantidad',
                                          'moa_ipoconsumo_cantidad',
                                          'channel',
                                          'document_type'])
              
              # Guarda el DataFrame en un archivo CSV en la ruta definida en las variables de entorno
              wr.s3.to_csv(df_3, path=path_of_processed_file_3, index=False, sep=';')
              
              # Dataframe de Quantity
              df_4 = pd.DataFrame(items_quantity_data,
                                  columns=['id_orden',
                                  'id_empresa',
                                  'consecutivo',
                                  'id_tipo_cantidad',
                                  'cantidad',
                                  'id_unidad_medida', 
                                  'fcha_actlzcn_regstr', 
                                  'pais',
                                  'file_type',
                                  'channel',
                                  'document_type'])
              
              # Guarda el DataFrame en un archivo CSV en la ruta definida en las variables de entorno
              wr.s3.to_csv(df_4, path=path_of_processed_file_4, index=False, sep=';')
              
              items_data = []
              items_quantity_data = []
              purchase_order_data = []
              purchase_order_status_data = []
              detail_data = []
  
              head_nad_by = None
              head_nad_su = None
              head_nad_dp = None
              head_bgm_message_type = None
              head_bgm_document_id = None
              head_dtm_document_date = None
              head_dtm_minimum_date = None
              head_dtm_maximum_date = None
              head_loc_place_identificacion = None
  
              det_ean_lin = None
              det_consecutive_lin = 0
              det_pri_aaa = None
              det_pri_aab = None
              det_pia_sa = None
              det_tax_iva = None
              det_tax_ipoconsumo = None
              det_moa_iva_quantity = None
              det_moa_ipoconsumo_quantity = None
              det_loc_lin = None
              det_todeliver_qty_type = None
              det_todeliver_quantity = None
              det_todeliver_measure_unit = None
              det_bonus_qty_type = None
              det_bonus_quantity = None
              det_bonus_qty_measure_unit = None
              det_requested_qty_type = None
              IS_FIRST_LIN = True
              
          elif segment.tag == 'BGM':
            head_bgm_message_type = segment.elements[0]
            head_bgm_document_id = segment.elements[1]
          
          elif segment.tag == 'NAD':
            if segment.elements[0] == 'BY':
                head_nad_by = segment.elements[1][0]
                # Get Trader name based on head_unb_sender value
                trader = get_name_company(country, head_nad_by, 'C', dynamodb_client)
            elif segment.elements[0] == 'SU':
                head_nad_su = validate_double_buzon(country, segment.elements[1][0], dynamodb_client)
                # Get Provider name based on head_unb_receiver value
                provider = get_name_company(country, head_nad_su, 'F', dynamodb_client)
                
            elif segment.elements[0] == 'DP':
                head_nad_dp = segment.elements[1][0]
                
          elif segment.tag == 'DTM':
            formatted_date = datetime.strptime((segment.elements[0][1][:8]), '%Y%m%d').strftime('%Y-%m-%d')
            if segment.elements[0][0] == '137':
                head_dtm_document_date = formatted_date
            elif segment.elements[0][0] == '64':
                head_dtm_minimum_date = formatted_date
            elif segment.elements[0][0] == '63':
                head_dtm_maximum_date = formatted_date
                
          elif (segment.tag == 'LIN'):
            # Si es el primer segmento LIN, aún no se crea una línea nueva
            if IS_FIRST_LIN:
              det_ean_lin = segment.elements[2][0]
              det_consecutive_lin = segment.elements[0]
              IS_FIRST_LIN = False
            # Si no es la primera línea, se crea una nueva línea con encabezado + detalle
            else:
              
              path_of_processed_file_3 = os.environ['bucket_of_processed_file']+file_name[:-4]+'_CONSEC_'+str(CUR_UNH+1)+'_L_3.csv'
              path_of_processed_file_4 = os.environ['bucket_of_processed_file']+file_name[:-4]+'_CONSEC_'+str(CUR_UNH+1)+'_L_4.csv'
              
              # Salto de linea en segmento LIN
              items_data.append([str(id_orden)+'_'+str(CUR_UNH+1),
                                id_empresa,
                                det_consecutive_lin,
                                det_ean_lin,
                                det_pia_sa,
                                det_tax_iva,
                                det_tax_ipoconsumo,
                                det_pri_aaa,
                                det_pri_aab,
                                load_date,
                                country,
                                'ITEMS_OC',
                                '',
                                '',
                                channel,
                                document_type])
              
              if (det_requested_qty_type == '21'):
                items_quantity_data.append([str(id_orden)+'_'+str(CUR_UNH+1),
                                            id_empresa,
                                            det_consecutive_lin,
                                            det_requested_qty_type,
                                            det_requested_quantity,
                                            det_requested_qty_measure_unit,
                                            load_date,country,
                                            'ITEMS_QUANTITY_OC',
                                            channel,
                                            document_type])
              
              if (det_bonus_qty_type == '192'):
                items_quantity_data.append([str(id_orden)+'_'+str(CUR_UNH+1),
                                            id_empresa,
                                            det_consecutive_lin,
                                            det_bonus_qty_type,
                                            det_bonus_quantity,
                                            det_bonus_qty_measure_unit,
                                            load_date,
                                            country,
                                            'ITEMS_QUANTITY_OC',
                                            channel,
                                            document_type])
                                          
              det_ean_lin = segment.elements[2][0]
              det_consecutive_lin = segment.elements[0]
              det_pri_aaa = None
              det_pri_aab = None
              det_pia_sa = None
              det_tax_iva = None
              det_tax_ipoconsumo = None
              det_moa_iva_quantity = None
              det_moa_ipoconsumo_quantity = None
              det_loc_lin = None
              det_todeliver_qty_type = None
              det_todeliver_quantity = None
              det_todeliver_measure_unit = None
              det_bonus_qty_type = None
              det_bonus_quantity = None
              det_bonus_qty_measure_unit = None
              det_requested_qty_type = None
              
          elif (segment.tag == 'QTY'):
            
            if (segment.elements[0][0] == '21'):
              det_requested_qty_type = '21'
              det_requested_quantity = segment.elements[0][1]
              try:
  	            det_requested_qty_measure_unit = segment.elements[0][2]
              except:
  	            det_requested_qty_measure_unit = None
            elif (segment.elements[0][0] == '192'):
              det_bonus_qty_type = '192'
              det_bonus_quantity = segment.elements[0][1]
              try:
  	            det_bonus_qty_measure_unit = segment.elements[0][2]
              except:
  	            det_bonus_qty_measure_unit = None
  	            
          elif (segment.tag == 'PRI'):
            if (segment.elements[0][0] == 'AAA'):
              det_pri_aaa = segment.elements[0][1]
            elif (segment.elements[0][0] == 'AAB'):
                det_pri_aab = segment.elements[0][1]
                
          elif (segment.tag == 'PIA'):
            if (segment.elements[1][1] == 'SA'):
              det_pia_sa = segment.elements[1][0]
              
          elif (segment.tag == 'TAX'):
            if (segment.elements[1] == 'VAT'):
              try:
                det_tax_iva = segment.elements[4][3]
              except:
                det_tax_iva = None
            elif (segment.elements[1] == 'GST'):
              try:
                det_tax_ipoconsumo = segment.elements[4][3]
              except:
                det_tax_ipoconsumo = None    
              
          # SEGMENT LOC - ENCABEZADO
          elif (segment.tag == 'LOC'):
            if (segment.elements[0] == '1'):
              head_loc_place_identificacion = segment.elements[1][0]
            elif (segment.elements[0] == '7'):
              det_loc_lin = segment.elements[1][0]
              
          # SEGMENT UNZ
          elif (segment.tag == 'UNZ'):
            CUR_UNH += 1
            unz_snrf = segment.elements[1]
            
            # Bucket donde quedan transformados los EDI en formato parquet
            path_of_processed_file_1 = os.environ['bucket_of_processed_file']+file_name[:-4]+'_CONSEC_'+str(CUR_UNH)+'_L_1.csv'
            path_of_processed_file_2 = os.environ['bucket_of_processed_file']+file_name[:-4]+'_CONSEC_'+str(CUR_UNH)+'_L_2.csv'
            path_of_processed_file_3 = os.environ['bucket_of_processed_file']+file_name[:-4]+'_CONSEC_'+str(CUR_UNH)+'_L_3.csv'
            path_of_processed_file_4 = os.environ['bucket_of_processed_file']+file_name[:-4]+'_CONSEC_'+str(CUR_UNH)+'_L_4.csv'
            
            items_data.append([str(id_orden)+'_'+str(CUR_UNH),
                              id_empresa,
                              det_consecutive_lin,
                              det_ean_lin,
                              det_pia_sa,
                              det_tax_iva,
                              det_tax_ipoconsumo,
                              det_pri_aaa,
                              det_pri_aab,
                              load_date,
                              country,
                              'ITEMS_OC',
                              '',
                              '',
                              channel,
                              document_type])
              
            if (det_requested_qty_type == '21'):
              items_quantity_data.append([str(id_orden)+'_'+str(CUR_UNH),
                                          id_empresa,
                                          det_consecutive_lin,
                                          det_requested_qty_type,
                                          det_requested_quantity,
                                          det_requested_qty_measure_unit,
                                          load_date,
                                          country,
                                          'ITEMS_QUANTITY_OC',
                                          channel,
                                          document_type])
              
            if (det_requested_qty_type == '192'):
              items_quantity_data.append([str(id_orden)+'_'+str(CUR_UNH),
                                          id_empresa,
                                          det_consecutive_lin,
                                          det_bonus_qty_type,
                                          det_bonus_quantity,
                                          det_bonus_qty_measure_unit,
                                          load_date,country,
                                          'ITEMS_QUANTITY_OC',
                                          channel,
                                          document_type])
              
            purchase_order_status_data.append([str(id_orden)+'_'+str(CUR_UNH), 
                                              id_empresa, 
                                              head_unb_sender, 
                                              head_unb_receiver, 
                                              load_date, 
                                              country,
                                              'ESTADOS_OC',
                                              channel,
                                              document_type])
  
            purchase_order_data.append([str(id_orden)+'_'+str(CUR_UNH), 
                                        id_empresa, 
                                        head_nad_by, 
                                        head_nad_su, 
                                        head_nad_dp, 
                                        head_bgm_message_type, 
                                        head_bgm_document_id, 
                                        head_dtm_document_date, 
                                        head_dtm_minimum_date, 
                                        head_dtm_maximum_date, 
                                        head_loc_place_identificacion, 
                                        load_date, 
                                        country,
                                        'OC',
                                        channel,
                                        document_type])
            
            # Dataframe de Estados Orden de Compra
            df_1 = pd.DataFrame(purchase_order_status_data, 
                                columns=['id_orden',
                                        'id_empresa', 
                                        'ean_buzon_emisor', 
                                        'ean_buzon_receptor', 
                                        'fcha_actlzcn_regstr', 
                                        'pais',
                                        'file_type',
                                        'channel',
                                        'document_type'])
        
            # Guarda el DataFrame en un archivo CSV en la ruta definida en las variables de entorno
            wr.s3.to_csv(df_1, path=path_of_processed_file_1, index=False, sep=';')
              
            # Dataframe de Orden de Compra
            df_2 = pd.DataFrame(purchase_order_data, 
                                columns=['id_orden',
                                        'id_empresa',
                                        'nad_by_comprador',
                                        'nad_su_entidad_proveedora',
                                        'nad_dp_lugar_entrega',
                                        'id_bgm_tipo_mensaje',
                                        'id_bgm_documento',
                                        'dtm_fecha_documento',
                                        'fecha_minima',
                                        'fecha_maxima',
                                        'loc_identificacion_lugar', 
                                        'fcha_actlzcn_regstr', 
                                        'pais',
                                        'file_type',
                                        'channel',
                                        'document_type'])
              
            # Guarda el DataFrame en un archivo CSV en la ruta definida en las variables de entorno
            wr.s3.to_csv(df_2, path=path_of_processed_file_2, index=False, sep=';')
                
            # Dataframe de Items OC
            df_3 = pd.DataFrame(items_data,
                                columns=['id_orden',
                                'id_empresa',
                                'consecutivo',
                                'ean_articulo_lin',
                                'pia_sa_codigo_empresa',
                                'tax_iva',
                                'tax_ipoconsumo',
                                'pri_precio_bruto',
                                'pri_precio_neto', 
                                'fcha_actlzcn_regstr', 
                                'pais', 
                                'file_type',
                                'moa_iva_cantidad',
                                'moa_ipoconsumo_cantidad',
                                'channel',
                                'document_type'])
              
            # Guarda el DataFrame en un archivo CSV en la ruta definida en las variables de entorno
            wr.s3.to_csv(df_3, path=path_of_processed_file_3, index=False, sep=';')
              
            # Dataframe de Quantity
            df_4 = pd.DataFrame(items_quantity_data,
                                columns=['id_orden',
                                        'id_empresa',
                                        'consecutivo',
                                        'id_tipo_cantidad',
                                        'cantidad',
                                        'id_unidad_medida', 
                                        'fcha_actlzcn_regstr', 
                                        'pais',
                                        'file_type',
                                        'channel',
                                        'document_type'])
              
            # Guarda el DataFrame en un archivo CSV en la ruta definida en las variables de entorno
            wr.s3.to_csv(df_4, path=path_of_processed_file_4, index=False, sep=';')
        
        # Iterate to find number of elements in dataframe and to add SNRF field value
        for index, row in enumerate(data):
          total_records += 1
          
        # Save EDI Files in backup path
        prefix = 'oc_pmd/'+'country='+country+'/'+'ean_provider='+head_unb_receiver+'/'+'ean_trader='+head_unb_sender+'/'+'year_load='+load_year+'/'+'month_load='+load_month+'/'+'day_load='+load_day
        bucket_connection.put_object(Bucket = os.environ['backup_bucket'], Body = document_content, Key = prefix+'/'+file_name)
        
        # DateTime for ending execution
        end_execution = datetime.now(tz=timeZone).strftime(format_date)
          
        # Register bad files in aud_load_audit table from dynamodb
        register_load_audit(country,file_name,head_bgm_document_id,unz_snrf,head_unb_receiver,head_unb_sender,load_date,load_day,load_time,load_month,load_year,prefix,provider,trader,head_dtm_document_date,head_dtm_document_date,processing_state,log_audit,start_execution,end_execution,total_records,state,dynamodb_client)
        
        # remove processed document from bucket
        bucket_connection.delete_object(Bucket = name_bucket, Key = path_file)
        
        return {'statusCode': 200}  
      
      except Exception as e:
        
        log_audit = str(e)
        processing_state = 'RECHAZADO POR ESTRUCTURA'
        
        # Register bad files in aud_load_audit table from dynamodb
        register_load_audit(country,file_name,head_bgm_document_id,unz_snrf,head_unb_receiver,head_unb_sender,load_date,load_day,load_time,load_month,load_year,prefix,provider,trader,head_dtm_document_date,head_dtm_document_date,processing_state,log_audit,start_execution,end_execution,total_records,state,dynamodb_client)
        
        # Move bad files to rejected bucket
        bucket_connection.put_object(Bucket = os.environ['error_processed_file'], Body = document_content, Key = 'oc_pmd/'+file_name)
        
        # Delete file from raw bucket
        bucket_connection.delete_object(Bucket = name_bucket, Key = path_file) 
       
  except:

      '''Esta función se encarga de obtener el error por el cual la función no pudo procesar,
      e insertarlo en la tabla de dynamodb llamada aud_services_errors'''
      catch_error(traceback, context, dateutil, datetime, boto3)
      raise

def get_filename_extension(my_file_name):
    myfile, myext = os.path.splitext(my_file_name)
    return myfile, myext.lstrip('.')

''' Función que permite guardar auditoria de los EDI recibidos por parte del cliente '''
def register_load_audit(country,file_name,bgm,snrf,ean_provider,ean_trader,load_date,load_day,load_hour,load_month,load_year,path_file,provider,trader,reported_start_date,reported_end_date,processing_state,log_audit,start_execution,end_execution,total_records,state,dynamodb_client):
  
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
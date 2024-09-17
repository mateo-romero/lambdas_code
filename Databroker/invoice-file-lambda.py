import json
import boto3
from lxml import etree
from io import BytesIO
import pandas as pd
import awswrangler as wr
from datetime import datetime
import dateutil
import urllib.parse
import os
import traceback
from catch_error import catch_error
import re


def lambda_handler(event, context):  

  # cliente de conexion a S3
  s3_client = boto3.client('s3')

  # diccionario con las carpetas a donde dejar los archivos
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

  # Obtenemos el diccionarios con los datos del archivo
  dict_path_file = get_path_file(event)

  try:    
    
    # declaramos los nombres de los espacion que se tienen para acceder a los datos
    dict_name_spaces_file = {

      'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
      'cdt': 'urn:DocumentInformation:names:specification:ubl:colombia:schema:xsd:DocumentInformationAggregateComponents-1',
      'cac':'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
      'chl':'urn:carvajal:names:specification:ubl:colombia:schema:xsd:CarvajalHealthComponents-1',
      'clm54217':'urn:un:unece:uncefact:codelist:specification:54217:2001',
      'clm66411':'urn:un:unece:uncefact:codelist:specification:66411:2001',
      'clmIANAMIMEMediaType':'urn:un:unece:uncefact:codelist:specification:IANAMIMEMediaType:2003',
      'cts':'urn:carvajal:names:specification:ubl:colombia:schema:xsd:CarvajalAggregateComponents-1', 
      'ds':'http://www.w3.org/2000/09/xmldsig#',
      'ext':'urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2',
      'grl':'urn:General:names:specification:ubl:colombia:schema:xsd:GeneralAggregateComponents-1',
      'ipt':'urn:DocumentInformation:names:specification:ubl:colombia:schema:xsd:InteroperabilidadPT-1',
      'qdt':'urn:oasis:names:specification:ubl:schema:xsd:QualifiedDatatypes-2',
      'sts':'dian:gov:co:facturaelectronica:Structures-2-1',
      'udt':'urn:un:unece:uncefact:data:specification:UnqualifiedDataTypesSchemaModule:2',
      'xades':'http://uri.etsi.org/01903/v1.3.2#',
      'xades141':'http://uri.etsi.org/01903/v1.4.1#', 
      'xsi':'http://www.w3.org/2001/XMLSchema-instance',
      'schemaLocation':'urn:oasis:names:specification:ubl:schema:xsd:Invoice-2http://docs.oasis-open.org/ubl/os-UBL-2.1/xsd/maindoc/UBL-Invoice-2.1.xsd',
      'xmlns':'urn:oasis:names:specification:ubl:schema:xsd:Invoice-2'
    }

    # Obtenemos el archivo tageado para poder acceder a los datos de manera mas optima
    xml_file = get_file(dict_path_file, s3_client)

    # sacams la informacion del tag en donde tenemos toda la data que necesitamos
    file_content = get_file_contet(xml_file, dict_name_spaces_file, context)

    # se obtiene el diccionario para registrar la auditoria
    dict_aud_load_audit_databroker = get_dict_aud_load_audit_databroker(file_content, dict_name_spaces_file)

    # diccionario del archivo procesando
    print(dict_aud_load_audit_databroker)

    # se registra el archivo en la tabla de auditoria
    insert_update_load_audit(dict_aud_load_audit_databroker, dict_path_file, dict_document_types)    
   
    # solo si es una factura de venta se genera el parquet
    if 'FV' == dict_aud_load_audit_databroker.get('document_type'):

      # se obtiene el dataframe con los datos de la factura y sus item
      df_invoice = get_invoice_data(file_content, dict_name_spaces_file)
    
      # generamos y guarmaos el archivo parquet 
      generate_parquet(df_invoice, dict_path_file, s3_client)

    # compiamos los archivos en el backup y eliminamos el que se esta procesando
    copy_files(dict_aud_load_audit_databroker, dict_path_file, s3_client)

    # TODO implement
    return {
    
          'statusCode': 200

    }
  
  except:

    catch_error(traceback, context, dateutil, datetime, boto3)
    raise



'''
Funcion que se encarga de obtener todos los registros relacionados con el archivo
que llego en el evento de s3
'''
def get_path_file(event):

  #obtenemos el body de la sqs
  # body_sqs = json.loads(event['Records'][0]['body'])

  # zona horaria  
  timeZone = dateutil.tz.gettz('America/Bogota')
  
  # fecha actual
  current_date = datetime.now(tz=timeZone)

  # Obtenemos las subcarpetas y el nombre del archivo cargado 
  path_file = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8') 

  # Primero hacemos una separacion por / para obtener las carpetas y nombre de archivo
  array_data_path = path_file.split('/')

  # Obtenemos el nombre del archivo
  file_name = array_data_path[-1]

  # Obtenemos la extension del archivo
  extension_file = os.path.splitext(file_name)[1][1:]

  # diccionario con lo datos de rutas para procesar los archivos
  dict_path_file = {

    'file_name': file_name.replace('.' + extension_file,''),
    'path_file_name': f"stg_invoice/{file_name.replace('.' + extension_file,'.parquet')}", 
    'extension_file': extension_file,
    'file_name_parquet': file_name.replace('.' + extension_file,'.parquet'),
    'name_bucket_raw': event['Records'][0]['s3']['bucket']['name'],
    'path_file_raw' : path_file, 
    'bucket_invoice_parquet': os.environ['bucket_invoice_parquet'],
    'path_file_stage': 'stg_invoice',
    'load_date': current_date.strftime('%Y%m%d'),
    'load_time': current_date.strftime('%H%M%S'),
    'year':    str(current_date.year),
    'month':   str(current_date.month).zfill(2),
    'day':     str(current_date.day).zfill(2),
    'bucket_raw_backup': os.environ['bucket_raw_backup']
  
  }

  print('Processing_file >>> ', f"{dict_path_file.get('name_bucket_raw')}/{dict_path_file.get('path_file_raw')}")

  return dict_path_file



'''
  Funcion que se encarga de Obtener los datos del elemento buscado en la etiqueta 
  del xml
'''
def extract_data(element, dict_name_spaces_file, xml_content):

  xml_elemt = xml_content.find(element, dict_name_spaces_file)

  return xml_elemt.text if xml_elemt is not None else None


'''
  Funcion que se encarga de sacar los datos del archivo y regresar un datframe 
'''
def get_invoice_data(file_content, dict_name_spaces_file):

  # TODO datos de la identificacion del emisor 
  # Sacamos los datos de la infromacion de documento 
  sender_id_elem = file_content.find('.//cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cbc:CompanyID', dict_name_spaces_file)

  # se ontine el tipo de identificacion del emisor
  sender_document_type = sender_id_elem.get('schemeName') if sender_id_elem != None else None

  # se obtiene el digito de verificacion del emisor
  sender_check_digit = sender_id_elem.get('schemeID') if sender_id_elem != None else None

  # Obtenemos el numero de identificacion
  sender_id = sender_id_elem.text if sender_id_elem is not None else None

  # sacamos los datos del tag del regiomen del emisor
  sender_regimen_elem = file_content.find('.//cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cbc:TaxLevelCode', dict_name_spaces_file)
  
  # se obtiene el valor del regimen del emisor
  sender_regimen = sender_regimen_elem.get('listName') if sender_regimen_elem != None else None

  # TODO datos de la identificacion del receptor 
  # Sacamos los datos de la infromacion de documento 
  receptor_id_elem = file_content.find('.//cac:AccountingCustomerParty/cac:Party/cac:PartyTaxScheme/cbc:CompanyID', dict_name_spaces_file)

  # se ontine el tipo de identificacion del emisor
  receiver_document_type = receptor_id_elem.get('schemeName') if receptor_id_elem != None else None

  # se obtiene el digito de verificacion del emisor
  receiver_check_digit = sender_id_elem.get('schemeID') if receptor_id_elem != None else None

  # Obtenemos el numero de identificacion
  receptor_id = receptor_id_elem.text if receptor_id_elem is not None else None

  # sacamos los datos del tag del regimen del receptor
  receiver_regimen_elem = file_content.find('.//cac:AccountingCustomerParty/cac:Party/cac:PartyTaxScheme/cbc:TaxLevelCode', dict_name_spaces_file)
  
  # se obtiene el valor del regimen del receptor
  receiver_regimen = receiver_regimen_elem.get('listName') if receiver_regimen_elem != None else None

  # obtenemos el listado de items del archivo
  array_items_elems = file_content.findall('.//cac:InvoiceLine', dict_name_spaces_file)

  # Obtenemos el listado de metodos d epago de la factura
  array_payment_method_elems = file_content.findall('.//cac:PaymentMeans', dict_name_spaces_file)

  # obtenemos el listado de terminos de pagos de la factura
  array_payment_terms_elems = file_content.findall('.//cac:PaymentTerms', dict_name_spaces_file)

  # pasamos la fecha de factura a fecha real 
  invoice_date = datetime.strptime(extract_data('.//cbc:IssueDate', dict_name_spaces_file, file_content), '%Y-%m-%d')

  # Obtenemos el dataframe con los items de la factura
  array_invoice = get_items_invoice(array_items_elems, dict_name_spaces_file)

  # pasamos la lista de items a un data frame
  df_invoice = pd.DataFrame(array_invoice, dtype=str)

  # se obtiene el valor del los metodos de pago
  array_payment = get_payment_method(array_payment_method_elems, dict_name_spaces_file)

  # se Obtiene la lista de los terminos de pago
  array_payment_term = get_payment_terms(array_payment_terms_elems, dict_name_spaces_file)
  
  # agregamos el listado de metodos de pago
  df_invoice['payment'] = [array_payment] * len(df_invoice) if len(array_payment) > 0 else None

  # agregamos los terminos de pago
  df_invoice['payment_term'] = [array_payment_term] * len(df_invoice) if len(array_payment_term) > 0 else None

  #agregar los datos de encabezados de la factura 
  df_invoice['invoice_number']         = extract_data('.//cbc:ID', dict_name_spaces_file, file_content)
  df_invoice['invoice_prefix']         = extract_data('.//ext:UBLExtensions/ext:UBLExtension/ext:ExtensionContent/sts:DianExtensions/sts:InvoiceControl/sts:AuthorizedInvoices/sts:Prefix', dict_name_spaces_file, file_content)
  df_invoice['invoice_type_code']      = extract_data('.//cbc:InvoiceTypeCode', dict_name_spaces_file, file_content)
  df_invoice['invoice_date']           = invoice_date
  df_invoice['invoice_time']           = extract_data('.//cbc:IssueTime', dict_name_spaces_file, file_content)
  df_invoice['invoice_expiration_date']= extract_data('.//cbc:DueDate', dict_name_spaces_file, file_content)
  df_invoice['invoice_cufe']           = extract_data('.//cbc:UUID', dict_name_spaces_file, file_content)
  df_invoice['invoice_currency_code']  = extract_data('.//cbc:DocumentCurrencyCode', dict_name_spaces_file, file_content)
  df_invoice['invoice_total_value']    = extract_data('.//cac:LegalMonetaryTotal/cbc:PayableAmount', dict_name_spaces_file, file_content)
  df_invoice['sender_id']              = sender_id.strip()
  df_invoice['sender_check_digit']     = sender_check_digit
  df_invoice['sender_document_type']   = sender_document_type
  df_invoice['sender_regimen']         = sender_regimen
  df_invoice['sender_type_identification'] = extract_data('.//cac:AccountingSupplierParty/cbc:AdditionalAccountID', dict_name_spaces_file, file_content)
  df_invoice['sender_name']            = extract_data('.//cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cbc:RegistrationName', dict_name_spaces_file, file_content)
  df_invoice['sender_city']            = extract_data('.//cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cac:RegistrationAddress/cbc:ID', dict_name_spaces_file, file_content)
  df_invoice['sender_city_name']       = extract_data('.//cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cac:RegistrationAddress/cbc:CityName', dict_name_spaces_file, file_content)
  df_invoice['sender_department']      = extract_data('.//cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cac:RegistrationAddress/cbc:CountrySubentityCode', dict_name_spaces_file, file_content)
  df_invoice['sender_department_name'] = extract_data('.//cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cac:RegistrationAddress/cbc:CountrySubentity', dict_name_spaces_file, file_content)
  df_invoice['sender_country']         = extract_data('.//cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cac:RegistrationAddress/cac:Country/cbc:IdentificationCode', dict_name_spaces_file, file_content)
  df_invoice['sender_postal_zone']     = extract_data('.//cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cac:RegistrationAddress/cbc:PostalZone', dict_name_spaces_file, file_content)
  df_invoice['receiver_id']            = receptor_id.strip()
  df_invoice['receiver_check_digit']   = receiver_check_digit
  df_invoice['receiver_document_type'] = receiver_document_type
  df_invoice['receiver_regimen']       = receiver_regimen
  df_invoice['receiver_type_identification'] = extract_data('.//cac:AccountingCustomerParty/cbc:AdditionalAccountID', dict_name_spaces_file, file_content)
  df_invoice['receiver_name']          = extract_data('.//cac:AccountingCustomerParty/cac:Party/cac:PartyTaxScheme/cbc:RegistrationName', dict_name_spaces_file, file_content)
  df_invoice['receiver_city']          = extract_data('.//cac:AccountingCustomerParty/cac:Party/cac:PhysicalLocation/cac:Address/cbc:ID', dict_name_spaces_file, file_content)
  df_invoice['receiver_city_name']     = extract_data('.//cac:AccountingCustomerParty/cac:Party/cac:PhysicalLocation/cac:Address/cbc:CityName', dict_name_spaces_file, file_content)
  df_invoice['receiver_department']    = extract_data('.//cac:AccountingCustomerParty/cac:Party/cac:PhysicalLocation/cac:Address/cbc:CountrySubentityCode', dict_name_spaces_file, file_content)
  df_invoice['receiver_department_name'] = extract_data('.//cac:AccountingCustomerParty/cac:Party/cac:PhysicalLocation/cac:Address/cbc:CountrySubentity', dict_name_spaces_file, file_content)
  df_invoice['receiver_country']       = extract_data('.//cac:AccountingCustomerParty/cac:Party/cac:PhysicalLocation/cac:Address/cac:Country/cbc:IdentificationCode', dict_name_spaces_file, file_content)
  df_invoice['year']                   = str(invoice_date.year)
  df_invoice['month']                  = str(invoice_date.month).zfill(2)
  df_invoice['day']                    = str(invoice_date.day).zfill(2)  
  
  # regresamos la factura con sus items
  return df_invoice



'''
Funcon que se encarga de recorrer el listado de itams de la fctura y regresar un dataframe 
con sus respectivos impuestos aplicados a cada registro
'''
def get_items_invoice(array_items_elems, dict_name_spaces_file):

  # declaramos el arreglo de items de la factura
  array_items = []

  # recorremos el listado de elementos de la factura
  for item_elem in array_items_elems:

    # declaro el arreglo de impuestos para ese item de factura
    array_tax = []

    # declaro el arreglo de impuestos asumidos
    array_tax_asumed = []

    # declaro el arreglo de descuentos o recargados para ese item de la factura
    array_charge = []

    # declaramos el objecto que guardara la informacion del item de la factura 
    item_info = {

      'item_id'         : extract_data('.//cac:Item/cac:StandardItemIdentification/cbc:ID', dict_name_spaces_file, item_elem),
      'item_quantity'   : extract_data('.//cbc:InvoicedQuantity', dict_name_spaces_file, item_elem),
      'item_unit_price' : extract_data('.//cac:Price/cbc:PriceAmount', dict_name_spaces_file, item_elem),
      'item_total_price': extract_data('.//cbc:LineExtensionAmount', dict_name_spaces_file, item_elem),
      'item_consecutive': extract_data('.//cbc:ID', dict_name_spaces_file, item_elem),
      'item_description': validate_none_value(extract_data('.//cac:Item/cbc:Description', dict_name_spaces_file, item_elem))

    }

    # Obtenemos el listado de impuestos cobrados
    array_items_tax = item_elem.findall('.//cac:TaxTotal', dict_name_spaces_file)

    # Obtenemos el listado de los impuestos asumidos
    array_items_tax_asumed = item_elem.findall('.//cac:WithholdingTaxTotal', dict_name_spaces_file)

    # Obtenemos el listado de los impuestos asumidos
    array_items_charge = item_elem.findall('.//cac:AllowanceCharge', dict_name_spaces_file)
      
    # Obtenemos los valores de item con impuestos cobrados
    array_tax.extend(get_tax_items(array_items_tax, item_info, dict_name_spaces_file))

    # Obtenemos los valores de item con impuestos asumido
    array_tax_asumed.extend(get_tax_items(array_items_tax_asumed, item_info, dict_name_spaces_file))

    # Obtenemos los valores de item con los descuentos y recargos
    array_charge.extend(get_charge_items(array_items_charge, dict_name_spaces_file))

    # agregamos los impuestos a la columna tex del diccinario
    item_info['tax'] = array_tax if len(array_tax) > 0 else None

    # agregamos los impuestos asumidos a la columna tax_asumed del diccinario
    item_info['tax_asumed'] = array_tax_asumed if len(array_tax_asumed) > 0 else None

    # se agrega el listado de recargos o descuentos
    item_info['charge'] = array_charge if len(array_charge) > 0 else None

    array_items.append(item_info)

  # regresamos el listado de teams de la factura
  return array_items



'''
  Funcion que se encarga de obtener el listado de impuestos cobrados
'''
def get_tax_items(array_items_tax, item_info, dict_name_spaces_file):

  # Listado de impuestos
  array_tax = []
  
  #recorremos el listado
  for item_tax in array_items_tax:

    item_info_tax = {
      'tax_id'        : extract_data('.//cac:TaxSubtotal/cac:TaxCategory/cac:TaxScheme/cbc:ID', dict_name_spaces_file, item_tax),
      'tax'           : validate_none_value(extract_data('.//cac:TaxSubtotal/cac:TaxCategory/cac:TaxScheme/cbc:Name', dict_name_spaces_file, item_tax)),
      'tax_percentage': validate_none_value(extract_data('.//cac:TaxSubtotal/cac:TaxCategory/cbc:Percent', dict_name_spaces_file, item_tax)),
      'tax_base'      : extract_data('.//cac:TaxSubtotal/cbc:TaxableAmount', dict_name_spaces_file, item_tax),
      'tax_value'     : extract_data('.//cac:TaxSubtotal/cbc:TaxAmount', dict_name_spaces_file, item_tax),
    }

    # se agraga a la lista el item con impuestos
    array_tax.append(item_info_tax)

  # se regresa el listado de item con impuestos  
  return array_tax



'''
funcion que se encarga de ontner los cargos asignados al item de la factura puede ser descuento o recargos
'''
def get_charge_items(array_items_charge, dict_name_spaces_file):

  # declaramos la lista de cargos
  array_charge = []

  # recorremos el listado de cargos
  for item_charge in array_items_charge:

    item_info_charge = {

      'charge_id'        : extract_data('.//cbc:ID', dict_name_spaces_file, item_charge),
      'charge'           : validate_none_value(extract_data('.//cbc:AllowanceChargeReason', dict_name_spaces_file, item_charge)),
      'charge_type'      : 'Descuento' if extract_data('.//cbc:ChargeIndicator', dict_name_spaces_file, item_charge) == 'false' else 'Recargo',
      'charge_percentage': validate_none_value(extract_data('.//cbc:MultiplierFactorNumeric', dict_name_spaces_file, item_charge)),
      'charge_base'      : extract_data('.//cbc:BaseAmount', dict_name_spaces_file, item_charge),
      'charge_value'     : extract_data('.//cbc:Amount', dict_name_spaces_file, item_charge),

    }

        # se agraga a la lista el item con cargos
    array_charge.append(item_info_charge)

  # se regresa el listado de item con cargos  
  return array_charge




'''
funcion que se encarga de obtener los metodos d epago d ela factura
'''
def get_payment_method(array_payment_method_elems, dict_name_spaces_file):

  # declaramos la lista de metodos de pago
  array_payment_method = []

  # recorremos el listado de cargos
  for payment_method in array_payment_method_elems:

    # se contruye el objecto
    item_info_payment_method = {

      'payment_type'     : 'Contado' if extract_data('.//cbc:ID', dict_name_spaces_file, payment_method) == '1' else 'Credito',
      'payment_code'     : extract_data('.//cbc:PaymentMeansCode', dict_name_spaces_file, payment_method),
      'payment_date_expiration'  : validate_none_value(extract_data('.//cbc:PaymentDueDate', dict_name_spaces_file, payment_method)),
    }

        # se agraga a la lista el item con cargos
    array_payment_method.append(item_info_payment_method)

  # se regresa el listado de item con cargos  
  return array_payment_method


'''
funcion que se encarga de obtener los terminos de pago de la factura
'''
def get_payment_terms(array_payment_terms_elems, dict_name_spaces_file):

  # declaramos la lista de condiciones de pago
  array_payment_terms = []

  # recorremos el listado de condiciones
  for payment_terms in array_payment_terms_elems:  

    # se obtienen los datos de los terminos de pago
    payment_term_code = extract_data('.//cbc:ID', dict_name_spaces_file, payment_terms),    
    payment_term      = extract_data('.//cbc:Note', dict_name_spaces_file, payment_terms),
    payment_discount  = extract_data('.//cbc:SettlementDiscountPercent', dict_name_spaces_file, payment_terms),
    payment_penalty   = extract_data('.//cbc:PenaltySurchargePercent', dict_name_spaces_file, payment_terms),
    payment_value     = extract_data('.//cbc:Amount', dict_name_spaces_file, payment_terms),
    item_info_payment_terms = {
      
      'payment_term_code': validate_none_value(payment_term_code[0]),
      'payment_term'     : validate_none_value(payment_term[0]),
      'payment_term_discount_percentage' : validate_none_value(payment_discount[0]),
      'payment_term_penalty_percentage'  : validate_none_value(payment_penalty[0]),
      'payment_term_value'    : validate_none_value(payment_value[0]),
    }

    # se agraga a la lista el condiciones de pago
    array_payment_terms.append(item_info_payment_terms)

  # se regresa el listado de condiciones de pago
  return array_payment_terms



'''
  Funcion que se encarga de obtener el archivo de l bucket
'''
def get_file(dict_path_file, s3_client): 

  # Obtener el archivo del bucket S3
  file = s3_client.get_object(Bucket=dict_path_file.get('name_bucket_raw'), Key=dict_path_file.get('path_file_raw'))
    
  # Sacar el contenido del archivo
  file_content = file['Body'].read()

  # Expresión regular para quitar espacios de todos los URIs en los namespaces
  pattern = re.compile(rb'xmlns:[a-zA-Z0-9]+="[^"]*?\s+[^"]*?"')

  # removemos espacios es los namespaces del xml
  def remove_spaces(match):

    uri = match.group()

    return re.sub(rb'\s+', b'', uri)

  # Ajustamos todos los URIs en los namespaces
  corrected_content = re.sub(pattern, remove_spaces, file_content)

  # Declaramos la expresión para parsear el XML
  parser = etree.XMLParser(ns_clean=True, huge_tree=True)

  # Parseamos el contenido de la factura a estructura de tag para ser más fácil su identificación
  tree_file = etree.parse(BytesIO(corrected_content), parser=parser)

  # Regresamos el archivo en estructura de tags
  return tree_file.getroot()



'''
  funcion que se encarga de recuperar los datos de la factura la cual esta insertada en el xml del archivo 
  AttachedDocument en una de las etiquetas CDATA
'''
def get_file_contet(xml_file, dict_name_spaces_file, context):
  
  # declaramos la variable que obtendra el archivo que necesitamos
  file_content = None
  
  #Obtenemos un arreglo de los tag de descripcion en el cual en uno de elos tenemos el contenido de la factura
  array_tag_description = xml_file.findall('.//cbc:Description', dict_name_spaces_file)
        
  try:

    # sacamos la infromacion de la factura que esta en la primera posicion del arreglo
    tag_description_content = array_tag_description[0].text.strip()

    # pasamos el contenido
    file_content = etree.fromstring(tag_description_content.encode('utf-8'))

  except etree.XMLSyntaxError as e:

    print('Error xml: ', e)
            
    # si alguna linea fallo guardamos el error
    catch_error(traceback, context, dateutil, datetime, boto3)

  return file_content


'''
Funcion que se encarga de generar un archivo parquet con los datos obtenidos del xml de la factura
'''
def generate_parquet(df_invoice, dict_path_file, s3_client):

  # verficamos que exita informacion para generar el parquet
  if not df_invoice.empty:

    # construimos el path donde dejaremos el archivo
    path_file = '/'.join([
        's3:/',
        dict_path_file.get('bucket_invoice_parquet'),
        dict_path_file.get('path_file_stage'),   
        dict_path_file.get('file_name_parquet')
    ])

    # agregamos los valores de fecha al dataframe
    df_invoice['load_date'] = dict_path_file.get('load_date')
    df_invoice['load_time'] = dict_path_file.get('load_time')
    df_invoice['file_name'] = dict_path_file.get('file_name_parquet')
    df_invoice['path_file_name'] = dict_path_file.get('path_file_name')
    df_invoice['bucket_name'] = dict_path_file.get('bucket_invoice_parquet')

    invoice_dtypes = {
      
      'invoice_number': 'string',
      'invoice_prefix': 'string',
      'invoice_date': 'string',
      'invoice_time': 'string',
      'invoice_expiration_date': 'string',
      'invoice_cufe': 'string',
      'invoice_type_code': 'string',
      'invoice_currency_code': 'string',
      'invoice_total_value': 'double',
      'receiver_id': 'string',
      'receiver_check_digit': 'string',
      'receiver_document_type': 'string',
      'receiver_regimen': 'string',
      'receiver_type_identification': 'string',
      'receiver_name': 'string',
      'receiver_city': 'string',
      'receiver_city_name': 'string',
      'receiver_department': 'string',
      'receiver_department_name': 'string',
      'receiver_country': 'string',
      'sender_id': 'string',
      'sender_check_digit': 'string',
      'sender_document_type': 'string',
      'sender_regimen': 'string',
      'sender_type_identification': 'string',
      'sender_name': 'string',
      'sender_city': 'string',
      'sender_city_name': 'string',
      'sender_department': 'string',
      'sender_department_name': 'string',
      'sender_country': 'string',
      'sender_postal_zone': 'string',
      'item_consecutive': 'string',
      'item_id': 'string',
      'item_description': 'string',
      'item_quantity': 'double',
      'item_unit_price': 'double',
      'item_total_price': 'double',
      'tax': 'string',
      'tax_asumed': 'string',
      'charge': 'string',
      'payment': 'string',
      'payment_term': 'string',
      'load_date': 'string',
      'load_time': 'string',
      'year': 'string',
      'month': 'string',
      'day': 'string',
      'file_name': 'string',
      'path_file_name': 'string',
      'bucket_name': 'string'
    }

    # se genera y se guarda el archivo el bucket para ser procesado
    wr.s3.to_parquet(dtype=invoice_dtypes,
                    df=pd.DataFrame(df_invoice, columns=['invoice_number','invoice_prefix','invoice_date','invoice_time','invoice_expiration_date','invoice_cufe','invoice_type_code','invoice_currency_code','invoice_total_value',
                                                        'receiver_id','receiver_check_digit','receiver_document_type','receiver_regimen','receiver_type_identification','receiver_name','receiver_city',
                                                        'receiver_city_name','receiver_department','receiver_department_name','receiver_country',
                                                        'sender_id','sender_check_digit','sender_document_type','sender_regimen','sender_type_identification','sender_name','sender_city',
                                                        'sender_city_name','sender_department','sender_department_name','sender_country','sender_postal_zone',
                                                        'item_consecutive','item_id','item_description','item_quantity','item_unit_price','item_total_price','tax','tax_asumed',
                                                        'charge','payment','payment_term','load_date','load_time','year','month','day','file_name','path_file_name','bucket_name'
                                                        ]),
                    path=path_file)


    # eliminamos el archivo procesado
    #delete_file(dict_path_file, s3_client)



'''
Funcion que se encarga de borrar el archivo xml del raw y el parquet generado
'''
def delete_file(dict_path_file, s3_client):

  # eliminamos el archivo del raw
  s3_client.delete_object(Bucket=dict_path_file.get('name_bucket_raw'), Key=dict_path_file.get('path_file_raw'))


'''
Esta función permite validar si el valor del parámetro "value" tiene el valor "None" y devuelve '' o "value" en caso contrario.
'''
def validate_none_value(value):

    return value if value is not None else ''



'''
funcion que se encarga de obtener el diccionario que se registrara en la tabla de aud_load_audit_databroker
'''
def get_dict_aud_load_audit_databroker(file_content, dict_name_spaces_file):
  
  # creamos un diccionario codigos de los tipos de documentos
  dict_tmp_document_types = {
        '01' : 'FV',
        '91' : 'NC',
        '033' : 'AE',
        '034' : 'AT',
        '032' : 'BS',
        '20': 'DEP',
        '05' : 'DS',
        '03' : 'FC',
        '04': 'FCD',
        '02' : 'FE',   
        '94': 'NAD',
        'ND' : 'ND',
        '95' : 'NS',
        '030' : 'RA',
        '031' : 'RF'
    }
  
  # se obtiene la fecha
  invoice_date = datetime.strptime(extract_data('.//cbc:IssueDate', dict_name_spaces_file, file_content), '%Y-%m-%d')

  # generamos el diccionario con los datos para insertar la auditoria
  dict_aud_load_audit_databroker = {

    'sender_id': get_sender(file_content, dict_name_spaces_file).strip(),
    'reiver_id': get_receiver(file_content, dict_name_spaces_file).strip(),
    'document_type': dict_tmp_document_types.get(get_document_type(file_content, dict_name_spaces_file), get_document_type(file_content, dict_name_spaces_file)),
    'email_receiver': get_email_receiver(file_content, dict_name_spaces_file),
    'cufe': get_cufe(file_content, dict_name_spaces_file),
    'extension': 'xml',
    'invoice_date': invoice_date.strftime('%Y-%m-%d'),
    'year': str(invoice_date.year),
    'month': str(invoice_date.month).zfill(2),
    'day':   str(invoice_date.day).zfill(2),
    'region': 'CO',
    'state': 'ACTIVO',
    'processing_state': 'NO REGISTRADO',
  }

  return dict_aud_load_audit_databroker




'''
Funcion que se encarga de obtener el id del emisor de cualquier documento
'''
def get_sender(file_content, dict_name_spaces_file):

  # inciamos obteniendo el emisor asumiendo que es una factura lo que se esta procesando
  sender_id = extract_data('.//cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cbc:CompanyID', dict_name_spaces_file, file_content)

  # verificamos si ya tenemos el dato del emisor
  if sender_id is None:
    
    sender_id = extract_data('.//cac:SenderParty/cac:PartyTaxScheme/cbc:CompanyID', dict_name_spaces_file, file_content)

  # regreso el valor de emisor
  return validate_none_value(sender_id)


'''
Funcion que se encarga de obtener el id del receptor de cualquier documento
'''
def get_receiver(file_content, dict_name_spaces_file):

  # inciamos obteniendo el receptor asumiendo que es una factura lo que se esta procesando
  receiver_id = extract_data('.//cac:AccountingCustomerParty/cac:Party/cac:PartyTaxScheme/cbc:CompanyID', dict_name_spaces_file, file_content)

  # verificamos si ya tenemos el dato del emisor
  if receiver_id is None:
    
    receiver_id = extract_data('.//cac:ReceiverParty/cac:PartyTaxScheme/cbc:CompanyID', dict_name_spaces_file, file_content)

  # regreso el valor de emisor
  return validate_none_value(receiver_id)


'''
Funcion que se encarga de obtener el tipo de documento de cualquier documento
'''
def get_document_type(file_content, dict_name_spaces_file):

  # inciamos obteniendo el receptor asumiendo que es una factura lo que se esta procesando
  document_type = extract_data('.//cbc:InvoiceTypeCode', dict_name_spaces_file, file_content)

  # verificamos si ya tenemos el dato del documento
  if document_type is None:
    
    document_type = extract_data('.//cbc:CreditNoteTypeCode', dict_name_spaces_file, file_content)

      # verificamos si ya tenemos el dato del documento
  if document_type is None:
    
    document_type = extract_data('.//cac:DocumentResponse/cac:Response/cbc:ResponseCode', dict_name_spaces_file, file_content)

  
  # verificamos si ya tenemos el dato del documento
  if document_type is None:
    
    document_type = extract_data('.//ext:UBLExtensions/ext:UBLExtension/ext:ExtensionContent/cdt:AdditionalDocumentInformation/cdt:Header/cdt:DocumentType', dict_name_spaces_file, file_content)

  # regreso el valor de emisor
  return validate_none_value(document_type)


'''
Funcion que se encarga de obtener el id del receptor de cualquier documento
'''
def get_email_receiver(file_content, dict_name_spaces_file):

  # inciamos obteniendo el receptor asumiendo que es una factura lo que se esta procesando
  email_receiver = extract_data('.//cac:AccountingCustomerParty/cac:Party/cac:Contact/cbc:ElectronicMail', dict_name_spaces_file, file_content)
  
  # verificamos si ya tenemos el dato del emisor
  if email_receiver is None:
    
    email_receiver = extract_data('.//cac:ReceiverParty/cac:Contact/cbc:ElectronicMail', dict_name_spaces_file, file_content)

  # regreso el valor de emisor
  return validate_none_value(email_receiver)



'''
Funcion que se encarga de obtener el id del receptor de cualquier documento
'''
def get_cufe(file_content, dict_name_spaces_file):

  # inciamos obteniendo el receptor asumiendo que es una factura lo que se esta procesando
  cufe = extract_data('.//cac:DocumentResponse/cac:DocumentReference/cbc:UUID', dict_name_spaces_file, file_content)
  
  # verificamos si ya tenemos el dato del emisor
  if cufe is None:
    
    cufe = extract_data('.//cac:BillingReference/cac:InvoiceDocumentReference/cbc:UUID', dict_name_spaces_file, file_content)

  
  # verificamos si ya tenemos el dato del emisor
  if cufe is None:
    
    cufe = extract_data('.//cbc:UUID', dict_name_spaces_file, file_content)

  # regreso el valor de emisor
  return validate_none_value(cufe)


'''
funcion que se encarga de registrar el archivo que se esta procesando el la base de datos
'''
def insert_update_load_audit(dict_aud_load_audit_databroker, dict_path_file, dict_document_types):

  path_backup = '/'.join([        
        dict_document_types.get(dict_aud_load_audit_databroker.get('document_type'), dict_aud_load_audit_databroker.get('document_type')),    
        f'''region=CO''',
        f'''receiver={dict_aud_load_audit_databroker.get('reiver_id')}''',
        f'''sender={dict_aud_load_audit_databroker.get('sender_id')}''',
        f'''year={dict_aud_load_audit_databroker.get('year')}''',
        f'''month={dict_aud_load_audit_databroker.get('month')}''',
        f'''day={dict_aud_load_audit_databroker.get('day')}'''        

    ])
  
  print('Destination_route >>> ', path_backup)
  
  dict_aud_load_audit_databroker['path_file'] = path_backup

  # # Crear un recurso DynamoDB
  dynamodb_client = boto3.client('dynamodb')

  dynamodb_client.put_item(
    TableName='aud_load_audit_databroker',
      Item={            
            'pk': {'S': f"receiver#{dict_aud_load_audit_databroker.get('reiver_id')}"},
            'sk': {'S': f"sender#{dict_aud_load_audit_databroker.get('sender_id')}#document_type#{dict_aud_load_audit_databroker.get('document_type')}#cufe#{dict_aud_load_audit_databroker.get('cufe')}"},
            'region':{'S': 'CO'},
            'receiver':{'S': dict_aud_load_audit_databroker.get('reiver_id').strip()},
            'email_receiver':{'S': dict_aud_load_audit_databroker.get('email_receiver')},
            'document_type':{'S': dict_aud_load_audit_databroker.get('document_type')},
            'sender':{'S': dict_aud_load_audit_databroker.get('sender_id')},
            'invoice_date':{'S': dict_aud_load_audit_databroker.get('invoice_date')},
            'cufe':{'S': dict_aud_load_audit_databroker.get('cufe')},
            'file_name':{'S': dict_path_file.get('file_name')},
            'extension':{'S': dict_aud_load_audit_databroker.get('extension')},
            'path_file':{'S': f"{dict_path_file.get('bucket_raw_backup')}/{path_backup}"},
            'state': {'S': 'ACTIVO'},
            'load_date': {'S': dict_path_file.get('load_date')},
            'load_time': {'S': dict_path_file.get('load_time')},
            'processing_state':{'S': 'NO REGISTRADO'},       
            'message_error':{'S': ''}
      }
  )


'''
funcion que se encarga de copiar el archivo del raw al bucket de backup
'''
def copy_files(dict_aud_load_audit_databroker, dict_path_file, s3_client):

  #copiamos el archivo del backup para el raw para ser procesado
  s3_client.copy_object(CopySource={'Bucket': dict_path_file.get('name_bucket_raw'), 'Key': dict_path_file.get('path_file_raw')},
                    Bucket= dict_path_file.get('bucket_raw_backup'), Key=f"{dict_aud_load_audit_databroker.get('path_file')}/{dict_path_file.get('file_name')}.xml")

  # eliminamos el archivo procesado
  delete_file(dict_path_file, s3_client)

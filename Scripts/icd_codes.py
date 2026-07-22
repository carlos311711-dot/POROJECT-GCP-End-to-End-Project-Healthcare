import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType

# Constants
CLIENT_ID = '09bf0f21-9dc3-41e0-966e-8ae3d476cc42_17a6ae0c-9a45-422d-a28b-796746818192'
CLIENT_SECRET = 'LygaivVEeV6GFKSgXOePgC7fB2eAf0aIxR2pqgtsPAQ='
TOKEN_ENDPOINT = 'https://icdaccessmanagement.who.int/connect/token'
API_VERSION = 'v2'
ACCEPT_LANGUAGE = 'en'
ROOT_URL = 'https://id.who.int/icd/release/10/2019/A00-A09'

# Función para obtener el token OAuth2
def get_access_token():
    payload = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scope': 'icdapi_access',
        'grant_type': 'client_credentials'
    }
    response = requests.post(TOKEN_ENDPOINT, data=payload, verify=False)
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        raise Exception(f"Failed to obtain access token: {response.status_code} - {response.text}")

# Función para hacer las peticiones a la API
def fetch_icd_codes(url, headers):
    response = requests.get(url, headers=headers, verify=True)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

# Función recursiva para extraer los códigos ICD
def extract_codes(url, headers):
    data = fetch_icd_codes(url, headers)
    codes = []
    if 'child' in data:
        for child_url in data['child']:
            codes.extend(extract_codes(child_url, headers))
    else:
        if 'code' in data and 'title' in data:
            codes.append({
                'icd_code': data['code'],
                'icd_code_type': 'ICD-10',
                'code_description': data['title']['@value'],
                'inserted_date': datetime.now().date(),
                'updated_date': datetime.now().date(),
                'is_current_flag': True
            })
    return codes

# Obtener el token de acceso
access_token = get_access_token()

# Configurar los headers de las peticiones a la API
headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/json',
    'Accept-Language': ACCEPT_LANGUAGE,
    'API-Version': API_VERSION
}

# Extraer los códigos ICD
icd_codes = extract_codes(ROOT_URL, headers)

# Definir el esquema
schema = StructType([
    StructField("icd_code", StringType(), True),
    StructField("icd_code_type", StringType(), True),
    StructField("code_description", StringType(), True),
    StructField("inserted_date", DateType(), True),
    StructField("updated_date", DateType(), True),
    StructField("is_current_flag", BooleanType(), True)
])

# Inicializar la sesión de Spark
spark = SparkSession.builder.appName("ICD_Codes_Extraction").getOrCreate()

# Crear el DataFrame
df = spark.createDataFrame(icd_codes, schema=schema)

# # Mostrar el DataFrame
# df.show()

# Guardar en Parquet
df.write.format("parquet").mode("append").save("gs://healthcare-bucket-22032025/landing/icd_codes/")

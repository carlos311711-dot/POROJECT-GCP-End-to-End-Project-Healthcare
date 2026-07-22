from datetime import date
import requests
from pyspark.sql import SparkSession

# No es necesario importar ni inicializar SparkSession en notebooks de Databricks
# from pyspark.sql import SparkSession

# Usar date.today() para obtener la fecha actual en un formato que Spark pueda manejar
current_date = date.today()

# Inicializar la sesión de Spark
spark = SparkSession.builder.appName("NPI Data").getOrCreate()

# URL base de la API del NPI Registry
base_url = "https://npiregistry.cms.hhs.gov/api/"

# Definir los parámetros para la petición inicial que trae la lista de NPIs
params = {
    "version": "2.1",  # versión de la API
    "state": "CA",  # estado de ejemplo, reemplazar por el deseado
    "city": "Los Angeles",  # ciudad de ejemplo, reemplazar por la deseada
    "limit": 20,  # límite de resultados para efectos de demostración
}

# Hacer la petición inicial para obtener la lista de NPIs
response = requests.get(base_url, params=params)

# Verificar si la petición fue exitosa
if response.status_code == 200:
    npi_data = response.json()
    npi_list = [result["number"] for result in npi_data.get("results", [])]

    # Inicializar una lista para guardar el detalle de cada NPI
    detailed_results = []

    # Recorrer cada NPI para obtener su detalle
    for npi in npi_list:
        detail_params = {"version": "2.1", "number": npi}
        detail_response = requests.get(base_url, params=detail_params)

        if detail_response.status_code == 200:
            detail_data = detail_response.json()
            if "results" in detail_data and detail_data["results"]:
                for result in detail_data["results"]:
                    npi_number = result.get("number")
                    basic_info = result.get("basic", {})
                    if result["enumeration_type"] == "NPI-1":
                        fname = basic_info.get("first_name", "")
                        lname = basic_info.get("last_name", "")
                    else:
                        fname = basic_info.get("authorized_official_first_name", "")
                        lname = basic_info.get("authorized_official_last_name", "")
                    position = (
                        basic_info.get("authorized_official_title_or_position", "")
                        if "authorized_official_title_or_position" in basic_info
                        else ""
                    )
                    organisation = basic_info.get("organization_name", "")
                    last_updated = basic_info.get("last_updated", "")
                    detailed_results.append(
                        {
                            "npi_id": npi_number,
                            "first_name": fname,
                            "last_name": lname,
                            "position": position,
                            "organisation_name": organisation,
                            "last_updated": last_updated,
                            "refreshed_at": current_date,
                        }
                    )

    # Crear el DataFrame
    if detailed_results:
        print(detailed_results)
        df = spark.createDataFrame(detailed_results)
#         display(df)
        df.write.format("parquet").mode("overwrite").save("gs://healthcare-bucket-22032025/landing/npi_extract/")

    else:
        print("No se encontraron resultados detallados.")
else:
    print(f"Error al obtener los datos: {response.status_code} - {response.text}")
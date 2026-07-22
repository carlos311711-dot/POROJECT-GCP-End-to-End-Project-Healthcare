import argparse
import glob
import os
import tempfile
from shutil import copytree, ignore_patterns
from google.cloud import storage

def _create_file_list(directory: str, name_replacement: str) -> tuple[str, list[str]]:
    """Copia los archivos relevantes a un directorio temporal y devuelve la lista."""
    if not os.path.exists(directory):
        print(f"⚠️ Advertencia: el directorio '{directory}' no existe. Se omite la subida.")
        return "", []  # Devolver valores vacíos para que el script no falle.

    temp_dir = tempfile.mkdtemp()
    files_to_ignore = ignore_patterns("__init__.py", "*_test.py")
    copytree(directory, f"{temp_dir}/", ignore=files_to_ignore, dirs_exist_ok=True)

    # Asegurar que solo se devuelvan archivos
    files = [f for f in glob.glob(f"{temp_dir}/**", recursive=True) if os.path.isfile(f)]
    return temp_dir, files

def upload_to_composer(directory: str, bucket_name: str, name_replacement: str) -> None:
    """Sube los DAGs o los archivos de datos al bucket de Cloud Storage de Composer."""
    temp_dir, files = _create_file_list(directory, name_replacement)

    if not files:
        print(f"⚠️ No se encontraron archivos en '{directory}'. Se omite la subida.")
        return  # Salir si no hay archivos disponibles.

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for file in files:
        file_gcs_path = file.replace(f"{temp_dir}/", name_replacement)
        try:
            blob = bucket.blob(file_gcs_path)
            blob.upload_from_filename(file)  # Asegurar que solo se suban archivos
            print(f"✅ Se subió {file} a gs://{bucket_name}/{file_gcs_path}")
        except IsADirectoryError:
            print(f"⚠️ Se omite el directorio: {file}")
        except FileNotFoundError:
            print(f"❌ Error: no se encontró {file}. Verifica que la estructura de directorios sea correcta.")
            raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sube los DAGs y los datos al bucket de Composer.")
    parser.add_argument("--dags_directory", help="Ruta al directorio de DAGs.")
    parser.add_argument("--dags_bucket", help="Nombre del bucket de GCS para los DAGs.")
    parser.add_argument("--data_directory", help="Ruta al directorio de datos.")

    args = parser.parse_args()

    print(args.dags_directory, args.dags_bucket, args.data_directory)

    if args.dags_directory and os.path.exists(args.dags_directory):
        upload_to_composer(args.dags_directory, args.dags_bucket, "dags/")
    else:
        print(f"⚠️ Se omite la subida de DAGs: no se encontró el directorio '{args.dags_directory}'.")

    if args.data_directory and os.path.exists(args.data_directory):
        upload_to_composer(args.data_directory, args.dags_bucket, "data/")
    else:
        print(f"⚠️ Se omite la subida de datos: no se encontró el directorio '{args.data_directory}'.")

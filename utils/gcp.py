import pandas as pd
from google.cloud import storage, secretmanager
from google.oauth2 import service_account
from utils.env_config import config
from utils.logger import get_logger

logger = get_logger(__name__)

# Cliente singleton
_storage_client = None

def get_storage_client():
    """Inicializa (una vez) y devuelve el cliente de Google Cloud Storage."""
    global _storage_client
    if _storage_client:
        return _storage_client

    try:
        if config.GOOGLE_APPLICATION_CREDENTIALS:
            credentials = service_account.Credentials.from_service_account_file(config.GOOGLE_APPLICATION_CREDENTIALS)
            _storage_client = storage.Client(credentials=credentials, project=config.GCP_PROJECT_NAME)
            logger.info("Cliente de Storage inicializado con archivo de credenciales.")
        else:
            _storage_client = storage.Client(project=config.GCP_PROJECT_NAME)
            logger.info("Cliente de Storage inicializado con Application Default Credentials (ADC).")
    except Exception as e:
        logger.error(f"Error al inicializar cliente de GCS: {e}")
        _storage_client = None

    return _storage_client


def upload_csv_to_gcs(dataframe: pd.DataFrame, bucket_name: str, gcs_file_path: str, content_type: str = 'text/csv'):
    """
    Sube un DataFrame de pandas como CSV a Google Cloud Storage.

    Args:
        dataframe: DataFrame de pandas.
        bucket_name: Nombre del bucket de GCS.
        gcs_file_path: Ruta del archivo en GCS.
        content_type: Tipo de contenido del archivo.

    Returns:
        bool: True si se subió correctamente, False en caso contrario.
    """
    client = get_storage_client()
    if not client:
        return False

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_file_path)
        csv_data = dataframe.to_csv(index=False, encoding='utf-8')
        blob.upload_from_string(csv_data, content_type=content_type)
        logger.info(f"Archivo subido con éxito a gs://{bucket_name}/{gcs_file_path}")
        return True
    except Exception as e:
        logger.error(f"Error al subir archivo a GCS: {e}")
        return False


def upload_parquet_to_gcs(dataframe: pd.DataFrame, bucket_name: str, gcs_file_path: str, content_type: str = 'application/octet-stream'):
    """
    Sube un DataFrame de pandas como Parquet a Google Cloud Storage.

    Args:
        dataframe: DataFrame de pandas.
        bucket_name: Nombre del bucket de GCS.
        gcs_file_path: Ruta del archivo en GCS.
        content_type: Tipo de contenido del archivo.

    Returns:
        bool: True si se subió correctamente, False en caso contrario.
    """
    client = get_storage_client()
    if not client:
        return False

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_file_path)
        
        # Crear el archivo Parquet en memoria
        import io
        parquet_buffer = io.BytesIO()
        dataframe.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_data = parquet_buffer.getvalue()
        
        blob.upload_from_string(parquet_data, content_type=content_type)
        logger.info(f"Archivo Parquet subido con éxito a gs://{bucket_name}/{gcs_file_path}")
        return True
    except Exception as e:
        logger.error(f"Error al subir archivo Parquet a GCS: {e}")
        return False


def get_secret(secret_id: str) -> str:
    """
    Recupera el valor de un secreto almacenado en Secret Manager.

    Args:
        secret_id: ID del secreto.

    Returns:
        Valor del secreto como string.
    """
    try:
        if config.GOOGLE_APPLICATION_CREDENTIALS:
            credentials = service_account.Credentials.from_service_account_file(config.GOOGLE_APPLICATION_CREDENTIALS)
            client = secretmanager.SecretManagerServiceClient(credentials=credentials)
            logger.info("Cliente de Secret Manager inicializado con archivo de credenciales.")
        else:
            client = secretmanager.SecretManagerServiceClient()
            logger.info("Cliente de Secret Manager inicializado con Application Default Credentials (ADC).")
        
        name = f"projects/{config.GCP_PROJECT_ID}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        logger.info(f"Secreto '{secret_id}' accedido correctamente.")
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Error al acceder al secreto '{secret_id}': {e}")
        return ""
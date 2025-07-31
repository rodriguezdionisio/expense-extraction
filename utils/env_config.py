import os

# Carga .env solo en local
if os.getenv("ENV", "local") == "local":
    from dotenv import load_dotenv
    load_dotenv()

class Config:
    GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
    GCP_PROJECT_NAME = os.getenv("GCP_PROJECT_NAME")
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", os.getenv("GOOGLE_CREDENTIALS_PATH"))
    
    # Configuraciones para extracción de expenses
    EXPENSE_EXTRACTION_MODE = os.getenv("EXPENSE_EXTRACTION_MODE", "maintenance")  # "initial" o "maintenance"
    EXPENSE_START_ID = int(os.getenv("EXPENSE_START_ID", "500"))  # ID inicial para extracción
    EXPENSE_PAGE_SIZE = int(os.getenv("EXPENSE_PAGE_SIZE", "500"))  # Tamaño de página por defecto
    EXPENSE_MAX_PAGES = int(os.getenv("EXPENSE_MAX_PAGES", "0"))  # 0 = sin límite
    
config = Config()

print(f"GCP_PROJECT_ID: {config.GCP_PROJECT_ID}")
print(f"GOOGLE_APPLICATION_CREDENTIALS: {config.GOOGLE_APPLICATION_CREDENTIALS}")
print(f"EXPENSE_EXTRACTION_MODE: {config.EXPENSE_EXTRACTION_MODE}")
print(f"EXPENSE_START_ID: {config.EXPENSE_START_ID}")
#print(f"FUDO_AUTH_URL: {config.FUDO_AUTH_URL}")
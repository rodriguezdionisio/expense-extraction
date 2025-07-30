import json
import requests
from datetime import datetime
from pathlib import Path
from utils.gcp import get_secret
from utils.logger import get_logger

logger = get_logger(__name__)

FUDO_AUTH_URL = "https://auth.fu.do/api"
FUDO_API_URL = "https://api.fu.do/v1alpha1"

class FudoRawExtractor:
    """Extractor de datos raw de la API de Fudo en formato JSON."""
    
    def __init__(self, output_dir="raw_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.token = None
        
    def get_token(self):
        """Obtiene el token de autenticación desde la API de Fudo."""
        try:
            api_key = get_secret("fudo-api-key")
            api_secret = get_secret("fudo-api-secret")

            payload = {"apiKey": api_key, "apiSecret": api_secret}
            headers = {"Content-Type": "application/json"}

            response = requests.post(FUDO_AUTH_URL, json=payload, headers=headers)
            response.raise_for_status()

            token = response.json().get("token")
            if not token:
                raise Exception("Token no encontrado en la respuesta.")

            logger.info("Token obtenido correctamente desde Fudo.")
            self.token = token
            return token

        except requests.exceptions.RequestException as e:
            logger.error(f"Error de conexión al obtener token: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al obtener token: {e}")
            raise

    def extract_raw_data(self, endpoint, extra_params=None, page_size=500, max_pages=None):
        """
        Extrae datos raw de un endpoint y los guarda en archivos JSON por página.
        
        Args:
            endpoint (str): Endpoint de la API (ej: "/expenses")
            extra_params (dict, optional): Parámetros adicionales
            page_size (int): Tamaño de página
            max_pages (int or None): Máximo de páginas a extraer
            
        Returns:
            dict: Resumen de la extracción
        """
        if not self.token:
            raise Exception("Token no disponible. Ejecute get_token() primero.")
            
        url = f"{FUDO_API_URL}{endpoint}"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        # Crear directorio específico para este endpoint
        endpoint_name = endpoint.strip("/").replace("/", "_")
        endpoint_dir = self.output_dir / endpoint_name
        endpoint_dir.mkdir(exist_ok=True)
        
        # Timestamp para esta extracción
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logger.info(f"Iniciando extracción de datos raw desde: {url}")
        
        current_page = 1
        pages_fetched = 0
        total_records = 0
        extraction_summary = {
            "endpoint": endpoint,
            "timestamp": timestamp,
            "pages_extracted": 0,
            "total_records": 0,
            "files_created": [],
            "extraction_metadata": {}
        }
        
        try:
            while True:
                params = {
                    "page[size]": page_size,
                    "page[number]": current_page
                }
                
                if extra_params:
                    params.update(extra_params)
                
                logger.info(f"Extrayendo página {current_page} con parámetros: {params}")
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                
                # Obtener la respuesta completa (raw JSON)
                page_data = response.json()
                results = page_data.get("data", [])
                
                # Guardar la página completa en JSON
                page_filename = f"{endpoint_name}_page_{current_page:04d}_{timestamp}.json"
                page_filepath = endpoint_dir / page_filename
                
                with open(page_filepath, 'w', encoding='utf-8') as f:
                    json.dump(page_data, f, indent=2, ensure_ascii=False)
                
                extraction_summary["files_created"].append(str(page_filepath))
                total_records += len(results)
                pages_fetched += 1
                
                logger.info(f"Página {current_page} guardada: {len(results)} registros en {page_filename}")
                
                # Verificar si hay más páginas
                if not results or len(results) < page_size:
                    logger.info("No hay más datos para extraer o se alcanzó el final.")
                    break
                
                current_page += 1
                
                if max_pages is not None and pages_fetched >= max_pages:
                    logger.info(f"Se alcanzó el máximo de páginas permitido: {max_pages}")
                    break
            
            # Actualizar resumen
            extraction_summary.update({
                "pages_extracted": pages_fetched,
                "total_records": total_records,
                "extraction_metadata": {
                    "page_size": page_size,
                    "last_page": current_page - 1,
                    "max_pages_limit": max_pages,
                    "extraction_params": extra_params
                }
            })
            
            # Guardar metadatos de la extracción
            metadata_filename = f"{endpoint_name}_extraction_metadata_{timestamp}.json"
            metadata_filepath = endpoint_dir / metadata_filename
            
            with open(metadata_filepath, 'w', encoding='utf-8') as f:
                json.dump(extraction_summary, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Extracción completada: {total_records} registros en {pages_fetched} páginas")
            return extraction_summary
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error durante la petición a la API: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado durante la extracción: {e}")
            raise

    def extract_expenses_raw(self, max_pages=None):
        """Extrae todos los datos raw de gastos."""
        logger.info("=== INICIANDO EXTRACCIÓN RAW DE GASTOS ===")
        
        extra_params = {
            "fields[expense]": "amount,canceled,cashRegister,createdAt,date,description,dueDate,expenseCategory,expenseItems,paymentMethod"
        }
        
        return self.extract_raw_data(
            endpoint="/expenses",
            extra_params=extra_params,
            max_pages=max_pages
        )

    def extract_payment_methods_raw(self):
        """Extrae datos raw de métodos de pago."""
        logger.info("=== INICIANDO EXTRACCIÓN RAW DE MÉTODOS DE PAGO ===")
        
        return self.extract_raw_data(
            endpoint="/payment-methods",
            page_size=100,
            max_pages=10
        )

    def extract_cash_registers_raw(self):
        """Extrae datos raw de cajas registradoras."""
        logger.info("=== INICIANDO EXTRACCIÓN RAW DE CAJAS REGISTRADORAS ===")
        
        return self.extract_raw_data(
            endpoint="/cash-registers",
            page_size=100,
            max_pages=10
        )

    def extract_expenses_by_date(self, start_date=None, end_date=None, partition_by="month"):
        """
        Extrae gastos ordenados por fecha y los agrupa por período.
        
        Args:
            start_date (str): Fecha de inicio en formato YYYY-MM-DD (opcional)
            end_date (str): Fecha de fin en formato YYYY-MM-DD (opcional) 
            partition_by (str): Criterio de partición: 'day', 'month', 'year'
            
        Returns:
            dict: Resumen de la extracción por fechas
        """
        from datetime import datetime, timedelta
        from collections import defaultdict
        import requests
        
        if not self.token:
            raise Exception("Token no disponible. Ejecute get_token() primero.")
            
        url = f"{FUDO_API_URL}/expenses"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        # Crear directorio para gastos por fecha
        expenses_dir = self.output_dir / "expenses_by_date"
        expenses_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logger.info(f"=== EXTRAYENDO GASTOS POR FECHA ===")
        logger.info(f"Partición: {partition_by}")
        logger.info(f"Período: {start_date or 'inicio'} a {end_date or 'fin'}")
        
        # Parámetros base - ordenar por fecha descendente
        base_params = {
            "sort": "-date",  # Más recientes primero
            "page[size]": 500,
            "fields[expense]": "amount,date,description,canceled,createdAt,dueDate"
        }
        
        # Agrupar registros por período
        date_groups = defaultdict(list)
        current_page = 1
        total_records = 0
        files_created = []
        
        try:
            while True:
                params = base_params.copy()
                params["page[number]"] = current_page
                
                logger.info(f"Extrayendo página {current_page}...")
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                
                page_data = response.json()
                records = page_data.get("data", [])
                
                if not records:
                    logger.info("No hay más registros")
                    break
                
                # Agrupar registros por fecha
                for record in records:
                    expense_date = record.get("attributes", {}).get("date")
                    if not expense_date:
                        continue
                        
                    # Convertir a datetime para agrupación
                    try:
                        date_obj = datetime.strptime(expense_date, "%Y-%m-%d")
                    except ValueError:
                        logger.warning(f"Fecha inválida: {expense_date}")
                        continue
                    
                    # Verificar rango de fechas
                    if start_date:
                        start_obj = datetime.strptime(start_date, "%Y-%m-%d")
                        if date_obj < start_obj:
                            logger.info(f"Llegamos a fecha anterior al inicio: {expense_date}")
                            break
                    
                    if end_date:
                        end_obj = datetime.strptime(end_date, "%Y-%m-%d")
                        if date_obj > end_obj:
                            continue
                    
                    # Generar clave de partición
                    if partition_by == "day":
                        partition_key = expense_date  # YYYY-MM-DD
                    elif partition_by == "month":
                        partition_key = date_obj.strftime("%Y-%m")  # YYYY-MM
                    elif partition_by == "year":
                        partition_key = str(date_obj.year)  # YYYY
                    else:
                        partition_key = expense_date
                    
                    date_groups[partition_key].append(record)
                
                total_records += len(records)
                current_page += 1
                
                # Si tenemos start_date y llegamos a esa fecha, parar
                if start_date and records:
                    last_date = records[-1].get("attributes", {}).get("date")
                    if last_date and last_date < start_date:
                        logger.info(f"Alcanzamos fecha de inicio: {last_date}")
                        break
                
                # Verificar si hay más páginas
                links = page_data.get("links", {})
                if not links.get("next"):
                    logger.info("No hay más páginas")
                    break
        
        except Exception as e:
            logger.error(f"Error durante extracción: {e}")
            raise
        
        # Guardar archivos por partición
        logger.info(f"Guardando {len(date_groups)} particiones...")
        
        for partition_key, records in date_groups.items():
            if not records:
                continue
                
            # Crear estructura de datos JSON:API
            partition_data = {
                "data": records,
                "meta": {
                    "partition": partition_key,
                    "partition_by": partition_by,
                    "record_count": len(records),
                    "extraction_timestamp": timestamp,
                    "date_range": {
                        "start": min(r.get("attributes", {}).get("date", "") for r in records),
                        "end": max(r.get("attributes", {}).get("date", "") for r in records)
                    }
                }
            }
            
            # Nombre del archivo
            filename = f"expenses_{partition_by}_{partition_key}_{timestamp}.json"
            filepath = expenses_dir / filename
            
            # Guardar archivo
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(partition_data, f, indent=2, ensure_ascii=False)
            
            files_created.append(str(filepath))
            logger.info(f"✅ Guardado {filename}: {len(records)} registros")
        
        # Crear archivo de metadatos
        metadata = {
            "extraction_type": "by_date",
            "partition_by": partition_by,
            "timestamp": timestamp,
            "date_range": {
                "start": start_date,
                "end": end_date
            },
            "summary": {
                "total_records": total_records,
                "partitions_created": len(date_groups),
                "files_created": len(files_created)
            },
            "files": files_created
        }
        
        metadata_file = expenses_dir / f"extraction_metadata_{partition_by}_{timestamp}.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ Metadatos guardados: {metadata_file}")
        
        return metadata

def main():
    """Función principal para ejecutar la extracción raw."""
    try:
        extractor = FudoRawExtractor()
        
        # Obtener token
        extractor.get_token()
        
        # Extraer gastos (todas las páginas)
        expenses_summary = extractor.extract_expenses_raw(max_pages=None)
        
        # Extraer métodos de pago
        try:
            payment_summary = extractor.extract_payment_methods_raw()
        except Exception as e:
            logger.warning(f"No se pudieron extraer métodos de pago: {e}")
            payment_summary = None
        
        # Extraer cajas registradoras
        try:
            cash_summary = extractor.extract_cash_registers_raw()
        except Exception as e:
            logger.warning(f"No se pudieron extraer cajas registradoras: {e}")
            cash_summary = None
        
        # Resumen final
        print("\n" + "="*60)
        print("RESUMEN DE EXTRACCIÓN RAW COMPLETADA")
        print("="*60)
        print(f"📊 GASTOS: {expenses_summary['total_records']} registros en {expenses_summary['pages_extracted']} páginas")
        
        if payment_summary:
            print(f"💳 MÉTODOS DE PAGO: {payment_summary['total_records']} registros")
        
        if cash_summary:
            print(f"🏪 CAJAS REGISTRADORAS: {cash_summary['total_records']} registros")
        
        print(f"\n📁 Archivos guardados en directorio: raw_data/")
        print("="*60)
        
        return {
            "expenses": expenses_summary,
            "payment_methods": payment_summary,
            "cash_registers": cash_summary
        }
        
    except Exception as e:
        logger.critical(f"Error crítico en la extracción: {e}")
        raise

if __name__ == "__main__":
    main()

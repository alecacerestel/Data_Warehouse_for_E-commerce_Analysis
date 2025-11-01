"""
Configuración de conexiones a bases de datos
"""
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import yaml

# Cargar variables de entorno
load_dotenv()

class DatabaseConfig:
    """Clase para manejar configuraciones de bases de datos"""
    
    def __init__(self):
        self.postgres_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.postgres_port = os.getenv('POSTGRES_PORT', '5432')
        self.postgres_user = os.getenv('POSTGRES_USER', 'postgres')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD', '')
        self.oltp_db = os.getenv('OLTP_DB', 'olist_oltp')
        self.dwh_db = os.getenv('DWH_DB', 'olist_dwh')
    
    def get_oltp_engine(self):
        """Retorna engine de SQLAlchemy para base OLTP"""
        connection_string = (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.oltp_db}"
        )
        return create_engine(connection_string, pool_pre_ping=True)
    
    def get_dwh_engine(self):
        """Retorna engine de SQLAlchemy para Data Warehouse"""
        connection_string = (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.dwh_db}"
        )
        return create_engine(connection_string, pool_pre_ping=True)
    
    def get_oltp_connection_string(self):
        """Retorna string de conexión para OLTP"""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.oltp_db}"
        )
    
    def get_dwh_connection_string(self):
        """Retorna string de conexión para DWH"""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.dwh_db}"
        )


def load_config(config_path='config/config.yaml'):
    """Carga configuración desde archivo YAML"""
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config


if __name__ == "__main__":
    # Test de conexión
    db_config = DatabaseConfig()
    print(f"OLTP Database: {db_config.oltp_db}")
    print(f"DWH Database: {db_config.dwh_db}")
    print(f"Host: {db_config.postgres_host}")
    
    try:
        oltp_engine = db_config.get_oltp_engine()
        print("✓ Conexión OLTP exitosa")
    except Exception as e:
        print(f"✗ Error en conexión OLTP: {e}")
    
    try:
        dwh_engine = db_config.get_dwh_engine()
        print("✓ Conexión DWH exitosa")
    except Exception as e:
        print(f"✗ Error en conexión DWH: {e}")

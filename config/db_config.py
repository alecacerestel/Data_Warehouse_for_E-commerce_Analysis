"""
Configuración de conexiones a base de datos OLTP
"""
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class DatabaseConfig:
    """Clase para manejar configuraciones de base de datos OLTP"""
    
    def __init__(self):
        self.postgres_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.postgres_port = os.getenv('POSTGRES_PORT', '5432')
        self.postgres_user = os.getenv('POSTGRES_USER', 'postgres')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD', '')
        self.oltp_db = os.getenv('OLTP_DB', 'olist_oltp')
    
    def get_oltp_engine(self):
        """Retorna engine de SQLAlchemy para base OLTP"""
        connection_string = (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.oltp_db}"
        )
        return create_engine(connection_string, pool_pre_ping=True)
    
    def get_oltp_connection_string(self):
        """Retorna string de conexión para OLTP"""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.oltp_db}"
        )


if __name__ == "__main__":
    # Test de conexión
    db_config = DatabaseConfig()
    print(f"OLTP Database: {db_config.oltp_db}")
    print(f"Host: {db_config.postgres_host}")
    
    try:
        oltp_engine = db_config.get_oltp_engine()
        print("✓ Conexión OLTP exitosa")
    except Exception as e:
        print(f"✗ Error en conexión OLTP: {e}")

"""
Configuración de conexiones a base de datos OLTP y OLAP
"""
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class DatabaseConfig:
    """Clase para manejar configuraciones de base de datos OLTP y OLAP"""
    
    def __init__(self):
        self.postgres_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.postgres_port = os.getenv('POSTGRES_PORT', '5432')
        self.postgres_user = os.getenv('POSTGRES_USER', 'postgres')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD', '')
        self.oltp_db = os.getenv('OLTP_DB', 'olist_oltp')
        self.olap_db = os.getenv('OLAP_DB', 'olist_olap')
    
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
    
    def get_olap_engine(self):
        """Retorna engine de SQLAlchemy para base OLAP"""
        connection_string = (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.olap_db}"
        )
        return create_engine(connection_string, pool_pre_ping=True)
    
    def get_olap_connection_string(self):
        """Retorna string de conexión para OLAP"""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.olap_db}"
        )


# Funciones auxiliares para facilitar importacion
def get_oltp_connection_string():
    """Retorna string de conexión OLTP"""
    db_config = DatabaseConfig()
    return db_config.get_oltp_connection_string()


def get_olap_connection_string():
    """Retorna string de conexión OLAP"""
    db_config = DatabaseConfig()
    return db_config.get_olap_connection_string()


if __name__ == "__main__":
    # Test de conexión
    db_config = DatabaseConfig()
    print(f"OLTP Database: {db_config.oltp_db}")
    print(f"OLAP Database: {db_config.olap_db}")
    print(f"Host: {db_config.postgres_host}")
    
    try:
        oltp_engine = db_config.get_oltp_engine()
        print("Conexion OLTP exitosa")
    except Exception as e:
        print(f"Error en conexion OLTP: {e}")
    
    try:
        olap_engine = db_config.get_olap_engine()
        print("Conexion OLAP exitosa")
    except Exception as e:
        print(f"Error en conexion OLAP: {e}")

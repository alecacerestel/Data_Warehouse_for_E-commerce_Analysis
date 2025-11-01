from config.db_config import DatabaseConfig
from sqlalchemy import text

config = DatabaseConfig()
engine = config.get_oltp_engine()

with engine.connect() as conn:
    # Ver la definición actual de la tabla geolocation
    result = conn.execute(text("""
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = 'geolocation'
        ORDER BY ordinal_position
    """))
    
    print("Estructura actual de la tabla 'geolocation' en la base de datos:")
    print("-" * 70)
    for row in result:
        col_name, data_type, max_len = row
        if max_len:
            print(f"{col_name:<30} {data_type:<20} MAX={max_len}")
        else:
            print(f"{col_name:<30} {data_type}")

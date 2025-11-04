"""
Script para verificar el modelo estrella creado en la fase de transformacion
Muestra estadisticas y samples de las dimensiones y tabla de hechos
"""
import pandas as pd
from pathlib import Path

def verify_transformed_data():
    """Verifica los datos transformados en data/transformed/"""
    
    transformed_path = Path("data/transformed")
    
    if not transformed_path.exists():
        print("ERROR: No existe el directorio data/transformed/")
        print("Ejecuta primero: python run_pipeline.py")
        return
    
    print("="*80)
    print("VERIFICACION DEL MODELO ESTRELLA")
    print("="*80)
    
    # Verificar dimensiones
    dimensions = [
        ('dim_customers.parquet', 'Dimension Clientes'),
        ('dim_products.parquet', 'Dimension Productos'),
        ('dim_sellers.parquet', 'Dimension Vendedores'),
        ('dim_date.parquet', 'Dimension Fecha')
    ]
    
    print("\nDIMENSIONES:")
    print("-" * 80)
    
    total_dim_records = 0
    for filename, name in dimensions:
        filepath = transformed_path / filename
        if filepath.exists():
            df = pd.read_parquet(filepath)
            total_dim_records += len(df)
            print(f"\n{name}: {filename}")
            print(f"  Registros: {len(df):,}")
            print(f"  Columnas: {len(df.columns)}")
            print(f"  Campos: {', '.join(df.columns.tolist())}")
            print(f"\n  Muestra de datos:")
            print(df.head(3).to_string(index=False))
        else:
            print(f"\n{name}: NO ENCONTRADO")
    
    # Verificar tabla de hechos
    print("\n" + "="*80)
    print("TABLA DE HECHOS:")
    print("-" * 80)
    
    fact_file = transformed_path / 'fct_orders.parquet'
    if fact_file.exists():
        df_fact = pd.read_parquet(fact_file)
        print(f"\nTabla de Hechos: fct_orders.parquet")
        print(f"  Registros: {len(df_fact):,}")
        print(f"  Columnas: {len(df_fact.columns)}")
        print(f"  Campos: {', '.join(df_fact.columns.tolist())}")
        
        print(f"\n  Estadisticas de Negocio:")
        print(f"    Valor total promedio: ${df_fact['order_total_value'].mean():.2f}")
        print(f"    Valor total minimo: ${df_fact['order_total_value'].min():.2f}")
        print(f"    Valor total maximo: ${df_fact['order_total_value'].max():.2f}")
        print(f"    Items promedio por orden: {df_fact['items_count'].mean():.2f}")
        print(f"    Tiempo de entrega promedio: {df_fact['delivery_time_days'].mean():.1f} dias")
        print(f"    Ordenes con retraso: {df_fact['is_delayed'].sum():,} ({df_fact['is_delayed'].mean()*100:.1f}%)")
        print(f"    Review score promedio: {df_fact['review_score'].mean():.2f}/5.0")
        
        print(f"\n  Distribucion por estado de orden:")
        status_counts = df_fact['order_status'].value_counts()
        for status, count in status_counts.items():
            print(f"    {status}: {count:,} ({count/len(df_fact)*100:.1f}%)")
        
        print(f"\n  Distribucion por tipo de pago:")
        payment_counts = df_fact['payment_type'].value_counts().head(5)
        for payment_type, count in payment_counts.items():
            print(f"    {payment_type}: {count:,} ({count/len(df_fact)*100:.1f}%)")
        
        print(f"\n  Muestra de datos:")
        print(df_fact[['order_id', 'customer_key', 'product_key', 'order_total_value', 
                      'review_score', 'is_delayed']].head(5).to_string(index=False))
        
        total_fact_records = len(df_fact)
    else:
        print("\nTabla de Hechos: NO ENCONTRADA")
        total_fact_records = 0
    
    # Resumen final
    print("\n" + "="*80)
    print("RESUMEN FINAL:")
    print("-" * 80)
    print(f"Total registros en dimensiones: {total_dim_records:,}")
    print(f"Total registros en tabla de hechos: {total_fact_records:,}")
    print(f"Total registros en modelo estrella: {total_dim_records + total_fact_records:,}")
    print(f"\nUbicacion: {transformed_path.absolute()}")
    print("="*80)


if __name__ == "__main__":
    verify_transformed_data()

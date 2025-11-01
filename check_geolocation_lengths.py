import pandas as pd

# Leer CSV
df = pd.read_csv('data/raw/olist_geolocation_dataset.csv')

print("Total registros:", len(df))
print("\nAnálisis de longitudes de texto:\n")

# Verificar cada columna de texto
for col in ['geolocation_zip_code_prefix', 'geolocation_city', 'geolocation_state']:
    df[col] = df[col].astype(str)
    max_len = df[col].str.len().max()
    idx_max = df[col].str.len().idxmax()
    valor_max = df.loc[idx_max, col]
    
    print(f"{col}:")
    print(f"  - Longitud máxima: {max_len}")
    print(f"  - Valor más largo: '{valor_max}'")
    print(f"  - Schema permite: ", end="")
    if col == 'geolocation_zip_code_prefix':
        print("VARCHAR(10)")
        if max_len > 10:
            print(f"  ⚠️  PROBLEMA! {max_len} > 10")
    elif col == 'geolocation_city':
        print("VARCHAR(100)")
        if max_len > 100:
            print(f"  ⚠️  PROBLEMA! {max_len} > 100")
    elif col == 'geolocation_state':
        print("CHAR(2)")
        if max_len > 2:
            print(f"  ⚠️  PROBLEMA! {max_len} > 2")
    print()

import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker("es_MX")


RAW_PATH = "data/raw/ventas.csv"
PROCESSED_PATH = "data/processed/ventas_limpias.csv"


def generar_datos(num_registros=200):
    data = []

    productos = ["Laptop", "Mouse", "Teclado", "Monitor", "", None]
    
    for i in range(1, num_registros + 1):
        registro = {
            "venta_id": i,
            "fecha": fake.date_between(start_date="-1y", end_date="today"),
            "producto": random.choice(productos),
            "cantidad": random.randint(1, 10),
            "precio": round(random.uniform(100, 5000), 2),
            "vendedor": fake.first_name()
        }
        data.append(registro)

    df = pd.DataFrame(data)
    os.makedirs("data/raw", exist_ok=True)
    df.to_csv(RAW_PATH, index=False)
    return df



def leer_datos():
    return pd.read_csv(RAW_PATH)



def filtrar_datos(df):
    total_registros = len(df)
    
    df_filtrado = df[
        df["producto"].notna() & (df["producto"].str.strip() != "")
    ]

    registros_finales = len(df_filtrado)
    eliminados = total_registros - registros_finales

    return df_filtrado, total_registros, eliminados



def guardar_datos(df):
    os.makedirs("data/processed", exist_ok=True)
    df.to_csv(PROCESSED_PATH, index=False)


def main():
    print("🚀 Iniciando proceso ETL...\n")

    print("📥 Generando datos de ventas...")
    generar_datos()

    print("📖 Leyendo datos...")
    df = leer_datos()

    print("🧹 Aplicando filtros de calidad...")
    df_limpio, total, eliminados = filtrar_datos(df)

    print("💾 Guardando datos procesados...")
    guardar_datos(df_limpio)

    print("\n📊 Resumen del ETL")
    print(f"Registros totales: {total}")
    print(f"Registros eliminados: {eliminados}")
    print(f"Registros finales: {len(df_limpio)}")

    print("\n✅ ETL finalizado correctamente")


if __name__ == "__main__":
    main()
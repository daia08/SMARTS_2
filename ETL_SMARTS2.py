# ==================================================================
#  ETL: DataBase "SMARTS"
#  Autoras: Laura Chaves, Fiorella Righelato, Daiana Gareis
#  GitHub: https://github.com/Laura-Chaves/Datawarehouse_SmartsTV
# ==================================================================

import pandas as pd
from sqlalchemy import create_engine, text

# Extracción
csv_file_path = 'Datos\Smart_TV_Data_v2.csv'
df = pd.read_csv(csv_file_path, delimiter=',')

# ===================================
#  Transformaciones para Dimensiones
# ===================================

df['Fecha'] = pd.to_datetime(df['Fecha'], format='%Y-%m-%d')
df[['Ciudad', 'Provincia']] = df['Localidad'].str.split(', ', expand=True)
df['Dia'] = df['Fecha'].dt.day
df['Mes'] = df['Fecha'].dt.month
df['Año'] = df['Fecha'].dt.year
df['Trimestre'] = df['Fecha'].dt.quarter
df.drop(columns=['Fecha', 'Localidad'], inplace=True)

# ===================================
#  Coneccion a la base de datos
# ===================================

# Conexión a la base de datos PostgreSQL
db_connection_str = 'postgresql+psycopg2://postgres:123@localhost:5432/SMARTS'
engine = create_engine(db_connection_str)

# ===================================
#  Inserciones a las dimensiones
# ===================================

# Función para obtener el ID de la dimensión
def get_dimension_id(query, params):
    with engine.connect() as connection:
        result = connection.execute(text(query), params).fetchone()
        return result[0] if result else None
    
# Función para insertar datos
def insert_data(engine, queries):
    with engine.connect() as connection:
        with connection.begin() as transaction:
            try:
                for query, params in queries:
                    connection.execute(text(query), params)
                transaction.commit()  # Confirmar la transacción al final
            except Exception as e:
                transaction.rollback()
                print(f"Error al insertar datos: {e}")

queries = []

for index, row in df.iterrows():
    # Instrucción SQL y parámetros para cada tabla
    queries.append((
        """
        INSERT INTO ubicacion (id_ubicacion, Ciudad, Provincia)
        VALUES (DEFAULT, :ciudad, :provincia)
        ON CONFLICT (Ciudad, Provincia) DO NOTHING;
        """,
        {'ciudad': row['Ciudad'], 'provincia': row['Provincia']}
    ))
    
    queries.append((
        """
        INSERT INTO plataforma (id_plataforma, Nombre_plataforma, Conexion)
        VALUES (DEFAULT, :plataforma, :conexion)
        ON CONFLICT (Nombre_plataforma) DO NOTHING;
        """,
        {'plataforma': row['Plataforma'], 'conexion': row['Conexion Exitosa']}
    ))
    
    queries.append((
        """
        INSERT INTO dispositivo (ID_Dispositivo, Tipo_dispositivo, Sistema_operativo)
        VALUES (DEFAULT, :dispositivo, :sistema_operativo)
        ON CONFLICT (Tipo_dispositivo) DO NOTHING;
        """,
        {'dispositivo': row['Dispositivo'], 'sistema_operativo': row['Sistema Operativo']}
    ))
    
    queries.append((
        """
        INSERT INTO tiempo (ID_Tiempo, Dia, Mes, Año, Trimestre)
        VALUES (DEFAULT, :dia, :mes, :año, :trimestre)
        ON CONFLICT (Dia, Mes, Año) DO NOTHING;
        """,
        {'dia': row['Dia'], 'mes': row['Mes'], 'año': row['Año'], 'trimestre': row['Trimestre']}
   
    ))

# Llamar a la función para ejecutar todas las inserciones en una transacción
insert_data(engine, queries)

print("Datos insertados correctamente.")

# ====================================================
#  Calcular conexiones exitosas y conexiones fallidas
# ====================================================

# Paso 1: Filtrar solo las conexiones exitosas y agrupar para obtener Total_conexiones_exitosas
df_filtered = df[df['Conexion Exitosa'] == 1]
grouped_consumo = df_filtered.groupby(['Dispositivo', 'Ciudad', 'Provincia', 'Trimestre', 'Plataforma']).size().reset_index(name='total_conexiones_exitosas')

# Paso 2: Calcular intentos_acceso_fallido en el dataset original y agregarlo al DataFrame
df['intentos_acceso_fallido'] = df['Conexion Exitosa'].apply(lambda x: 1 if x == 0 else 0)
failed_accesses = df.groupby(['Dispositivo', 'Ciudad', 'Provincia', 'Trimestre', 'Plataforma'])['intentos_acceso_fallido'].sum().reset_index()

# Combinar `total_conexiones_exitosas` y `intentos_acceso_fallido` en un solo DataFrame
grouped_consumo = grouped_consumo.merge(failed_accesses, on=['Dispositivo', 'Ciudad', 'Provincia', 'Trimestre', 'Plataforma'])

# Paso 3: Generar las columnas con los IDs de las dimensiones
grouped_consumo['ID_Tiempo'] = grouped_consumo['Trimestre'].apply(
    lambda trimestre: get_dimension_id("SELECT ID_Tiempo FROM tiempo WHERE Trimestre = :trimestre LIMIT 1", {'trimestre': trimestre})
)

grouped_consumo['ID_Ubicacion'] = grouped_consumo.apply(
    lambda row: get_dimension_id("SELECT ID_Ubicacion FROM ubicacion WHERE Ciudad = :ciudad AND Provincia = :provincia LIMIT 1", {'ciudad': row['Ciudad'], 'provincia': row['Provincia']}), axis=1
)

grouped_consumo['ID_Plataforma'] = grouped_consumo['Plataforma'].apply(
    lambda plataforma: get_dimension_id("SELECT ID_Plataforma FROM plataforma WHERE Nombre_plataforma = :plataforma LIMIT 1", {'plataforma': plataforma})
)

grouped_consumo['ID_Dispositivo'] = grouped_consumo['Dispositivo'].apply(
    lambda dispositivo: get_dimension_id("SELECT ID_Dispositivo FROM dispositivo WHERE Tipo_dispositivo = :dispositivo LIMIT 1", {'dispositivo': dispositivo})
)

# ====================================================
#  Verificacion de los resultados de las metricas
# ====================================================

# Paso 4: Verificación de totales en el dataset
total_conexiones_exitosas_original = df[df['Conexion Exitosa'] == 1].shape[0]
total_intentos_acceso_fallido_original = df['intentos_acceso_fallido'].sum()
total_conexiones_exitosas_agrupado = grouped_consumo['total_conexiones_exitosas'].sum()
total_intentos_acceso_fallido_agrupado = grouped_consumo['intentos_acceso_fallido'].sum()

# Verificar si ambos totales coinciden
print(f"Total de conexiones exitosas en el dataset original: {total_conexiones_exitosas_original}")
print(f"Total de 'total_conexiones_exitosas' en el dataset agrupado: {total_conexiones_exitosas_agrupado}")
print(f"Total de intentos de acceso fallido en el dataset original: {total_intentos_acceso_fallido_original}")
print(f"Total de 'intentos_acceso_fallido' en el dataset agrupado: {total_intentos_acceso_fallido_agrupado}")

# Guardar el DataFrame `grouped_consumo` en un archivo CSV
grouped_consumo.to_csv('Datos\total_conexiones.csv', index=False)
print("Archivo CSV 'total_conexiones.csv' guardado con éxito.")

# ============================================================
#  Actualización del proceso de upsert en la tabla de hechos
# ============================================================

# Crear las consultas de inserción en la tabla de hechos `consumo`
fact_queries = []

for index, row in grouped_consumo.iterrows():
    fact_queries.append((
        """
        INSERT INTO consumo (id_tiempo, id_ubicacion, id_plataforma, id_dispositivo, total_conexiones_exitosas, intentos_acceso_fallido)
        VALUES (:id_tiempo, :id_ubicacion, :id_plataforma, :id_dispositivo, :total_conexiones_exitosas, :intentos_acceso_fallido)
        ON CONFLICT (id_tiempo, id_ubicacion, id_plataforma, id_dispositivo) DO UPDATE
        SET total_conexiones_exitosas = EXCLUDED.total_conexiones_exitosas,
            intentos_acceso_fallido = EXCLUDED.intentos_acceso_fallido;
        """,
        {
            'id_tiempo': row['ID_Tiempo'],
            'id_ubicacion': row['ID_Ubicacion'],
            'id_plataforma': row['ID_Plataforma'],
            'id_dispositivo': row['ID_Dispositivo'],
            'total_conexiones_exitosas': row['total_conexiones_exitosas'],
            'intentos_acceso_fallido': row['intentos_acceso_fallido']
        }
    ))

# Llamar a la función `insert_data` para ejecutar las consultas de inserción en la tabla de hechos `consumo`
insert_data(engine, fact_queries)

print("Datos de la tabla de hechos 'consumo' insertados correctamente.")






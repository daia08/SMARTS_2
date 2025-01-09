import pandas as pd
from sqlalchemy import create_engine, text

# Extracción
csv_file_path = 'Smart_TV_Data_v2.csv'
df = pd.read_csv(csv_file_path, delimiter=',')

# Transformación
df['Fecha'] = pd.to_datetime(df['Fecha'], format='%Y-%m-%d')
df[['Ciudad', 'Provincia']] = df['Localidad'].str.split(', ', expand=True)
df['Dia'] = df['Fecha'].dt.day
df['Mes'] = df['Fecha'].dt.month
df['Año'] = df['Fecha'].dt.year
df['Trimestre'] = df['Fecha'].dt.quarter
df.drop(columns=['Fecha', 'Localidad'], inplace=True)

# Filtrar solo conexiones exitosas y agrupar para obtener Total_conexiones
df_filtered = df[df['Conexion Exitosa'] == 1]
grouped_consumo = df_filtered.groupby(['Dispositivo', 'Ciudad', 'Provincia', 'Trimestre', 'Plataforma']).size().reset_index(name='Total_conexiones')

# Conexión a la base de datos PostgreSQL
db_connection_str = 'postgresql+psycopg2://postgres:123@localhost:5432/SMARTS'
engine = create_engine(db_connection_str)

# Función para insertar datos en la base de datos usando transacciones
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

# Lista para almacenar las consultas de inserción
queries = []

# ===================================
#   Inserciones en las Dimensiones
# ===================================

# Iterar sobre cada fila del DataFrame para insertar datos en las tablas de dimensión
for index, row in df.iterrows():
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

# Ejecutar las inserciones de las dimensiones en la base de datos
insert_data(engine, queries)
print("Datos insertados correctamente en las dimensiones.")

# ==========================================
#   Agrupar Conexiones Exitosas y Guardar en CSV
# ==========================================

# Filtrar y agrupar conexiones exitosas
df_filtered = df[df['Conexion Exitosa'] == 1]
grouped_consumo = df_filtered.groupby(['Dispositivo', 'Ciudad', 'Provincia', 'Trimestre', 'Plataforma']).size().reset_index(name='Total_conexiones')

# Función para obtener el ID de una dimensión dada una consulta y parámetros
def get_dimension_id(query, params):
    with engine.connect() as connection:
        result = connection.execute(text(query), params).fetchone()
        return result[0] if result else None

# Asignar IDs a las dimensiones correspondientes en el DataFrame agrupado
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

# Mostrar totales de conexiones exitosas y guardar en CSV
total_conexiones_original = df[df['Conexion Exitosa'] == 1].shape[0]
total_conexiones_agrupado = grouped_consumo['Total_conexiones'].sum()
print(f"Total de conexiones exitosas en el dataset original: {total_conexiones_original}")
print(f"Total de 'Total_conexiones' en el dataset agrupado: {total_conexiones_agrupado}")

# Guardar el archivo CSV con las conexiones exitosas agrupadas
grouped_consumo.to_csv('total_conexiones.csv', index=False)
print("Archivo CSV 'total_conexiones.csv' guardado con éxito.")

# ================================================================
#   Insertar Conexiones Exitosas en la Tabla de Hechos
# ================================================================

# Leer datos de conexiones exitosas desde el CSV
df_fact_consumo = pd.read_csv('total_conexiones.csv')

# Función para insertar o actualizar datos en la tabla de hechos
def upsert_fact_table(engine, df):
    upsert_query = """
    INSERT INTO Consumo (ID_Tiempo, ID_Ubicacion, ID_Plataforma, ID_Dispositivo, Total_conexiones, Intentos_acceso_Plataformas)
    VALUES (:id_tiempo, :id_ubicacion, :id_plataforma, :id_dispositivo, :total_conexiones, 0)
    ON CONFLICT (ID_Tiempo, ID_Ubicacion, ID_Plataforma, ID_Dispositivo)
    DO UPDATE SET
        Total_conexiones = Consumo.Total_conexiones + EXCLUDED.Total_conexiones;
    """
    
    with engine.connect() as connection:
        with connection.begin() as transaction:
            try:
                for index, row in df.iterrows():
                    params = {
                        'id_tiempo': int(row['ID_Tiempo']),
                        'id_ubicacion': int(row['ID_Ubicacion']),
                        'id_plataforma': int(row['ID_Plataforma']),
                        'id_dispositivo': int(row['ID_Dispositivo']),
                        'total_conexiones': int(row['Total_conexiones'])
                    }
                    connection.execute(text(upsert_query), params)
                transaction.commit()
                print("Conexiones exitosas insertadas y/o actualizadas en la tabla de hechos.")
            except Exception as e:
                transaction.rollback()
                print(f"Error al insertar o actualizar datos en la tabla de hechos: {e}")

# Ejecutar inserción en la tabla de hechos
upsert_fact_table(engine, df_fact_consumo)

# =========================================
#   Insertar Conexiones Fallidas en la Tabla de Hechos
# =========================================

# Filtrar y agrupar conexiones fallidas
df_fallidas = df[df['Conexion Exitosa'] == 0]
matriz_fallidas = df_fallidas.groupby(
    ['Dispositivo', 'Ciudad', 'Provincia', 'Trimestre', 'Plataforma']
).size().reset_index(name='Total_conexiones_fallidas')

# Asignar IDs de dimensiones para conexiones fallidas
matriz_fallidas['ID_Tiempo'] = matriz_fallidas['Trimestre'].apply(
    lambda trimestre: get_dimension_id("SELECT ID_Tiempo FROM tiempo WHERE Trimestre = :trimestre LIMIT 1", {'trimestre': trimestre})
)
matriz_fallidas['ID_Ubicacion'] = matriz_fallidas.apply(
    lambda row: get_dimension_id("SELECT ID_Ubicacion FROM ubicacion WHERE Ciudad = :ciudad AND Provincia = :provincia LIMIT 1", {'ciudad': row['Ciudad'], 'provincia': row['Provincia']}), axis=1
)
matriz_fallidas['ID_Plataforma'] = matriz_fallidas['Plataforma'].apply(
    lambda plataforma: get_dimension_id("SELECT ID_Plataforma FROM plataforma WHERE Nombre_plataforma = :plataforma LIMIT 1", {'plataforma': plataforma})
)
matriz_fallidas['ID_Dispositivo'] = matriz_fallidas['Dispositivo'].apply(
    lambda dispositivo: get_dimension_id("SELECT ID_Dispositivo FROM dispositivo WHERE Tipo_dispositivo = :dispositivo LIMIT 1", {'dispositivo': dispositivo})
)

# Función para actualizar conexiones fallidas en la tabla de hechos
def update_failed_attempts(engine, df):
    update_query = """
    UPDATE Consumo
    SET Intentos_acceso_Plataformas = Intentos_acceso_Plataformas + :total_conexiones_fallidas
    WHERE ID_Tiempo = :id_tiempo
      AND ID_Ubicacion = :id_ubicacion
      AND ID_Plataforma = :id_plataforma
      AND ID_Dispositivo = :id_dispositivo;
    """
    
    with engine.connect() as connection:
        with connection.begin() as transaction:
            try:
                for index, row in df.iterrows():
                    params = {
                        'id_tiempo': int(row['ID_Tiempo']),
                        'id_ubicacion': int(row['ID_Ubicacion']),
                        'id_plataforma': int(row['ID_Plataforma']),
                        'id_dispositivo': int(row['ID_Dispositivo']),
                        'total_conexiones_fallidas': int(row['Total_conexiones_fallidas'])
                    }
                    connection.execute(text(update_query), params)
                transaction.commit()
                print("Intentos fallidos actualizados en la tabla de hechos.")
            except Exception as e:
                transaction.rollback()
                print(f"Error al actualizar intentos fallidos en la tabla de hechos: {e}")

# Ejecutar la actualización de conexiones fallidas
update_failed_attempts(engine, matriz_fallidas)
total_intentos_fallidos = matriz_fallidas['Total_conexiones_fallidas'].sum()
print("Total de intentos fallidos en matriz_fallidas:", total_intentos_fallidos)

# ===================================================
#  Finalización del ETL para la base de datos SMARTS
# ===================================================
print("ETL finalizado exitosamente.")


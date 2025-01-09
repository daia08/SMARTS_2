import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Ubicacion, Plataforma, Dispositivo, Tiempo, Consumo

# Extracción
csv_file_path = 'expanded_dataset.csv'
df = pd.read_csv(csv_file_path, delimiter=',')

# Transformación
df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y', errors='coerce')
df[['Ciudad', 'Provincia']] = df['Localidad'].str.split(', ', expand=True)
df['Dia'] = df['Fecha'].dt.day
df['Mes'] = df['Fecha'].dt.month
df['Año'] = df['Fecha'].dt.year
df['Trimestre'] = df['Fecha'].dt.quarter
df.drop(columns=['Fecha', 'Localidad'], inplace=True)

# Crear columna "Total_conexiones"
grouped = df[df['Conexion Exitosa'] == 1].groupby(['ID Usuario', 'Ciudad', 'Trimestre', 'Dispositivo', 'Plataforma']).size().reset_index(name='Total_conexiones')
df = df.merge(grouped, on=['ID Usuario', 'Ciudad', 'Trimestre', 'Dispositivo', 'Plataforma'], how='left')
df['Total_conexiones'] = df['Total_conexiones'].fillna(0)

# Configuración de la conexión a PostgreSQL
db_connection_str = 'postgresql+psycopg2://postgres:123@localhost:5432/SMARTS'
db_connection = create_engine(db_connection_str)
Session = sessionmaker(bind=db_connection)
session = Session()

# Cargar datos en la base de datos
for group, data in df.groupby(['Ciudad', 'Trimestre', 'Dispositivo', 'Plataforma']):
    ciudad, trimestre, dispositivo, plataforma = group
    total_conexiones = data['Total_conexiones'].iloc[0]

    ubicacion = insert_if_not_exists(session, Ubicacion, Ciudad=ciudad, Provincia=data['Provincia'].iloc[0])
    plataforma = insert_if_not_exists(session, Plataforma, Nombre_plataforma=plataforma, Conexion=data['Conexion Exitosa'].iloc[0])
    dispositivo = insert_if_not_exists(session, Dispositivo, Tipo_dispositivo=dispositivo, Sistema_operativo=data['Sistema Operativo'].iloc[0])
    tiempo = insert_if_not_exists(session, Tiempo, Dia=data['Dia'].iloc[0], Mes=data['Mes'].iloc[0], Año=data['Año'].iloc[0], Trimestre=trimestre)

    consumo = Consumo(
        ID_Tiempo=tiempo.ID_Tiempo,
        ID_Ubicacion=ubicacion.ID_Ubicacion,
        ID_Plataforma=plataforma.ID_Plataforma,
        ID_Dispositivo=dispositivo.ID_Dispositivo,
        Total_conexiones=total_conexiones
    )
    session.add(consumo)

session.commit()

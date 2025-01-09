------------------------------------
--   DIMENSIÓN UBICACIÓN
------------------------------------

CREATE TABLE Ubicacion (
    ID_Ubicacion SERIAL PRIMARY KEY,
    Ciudad VARCHAR(255),
    Provincia VARCHAR(255)
);

------------------------------------
--   DIMENSIÓN PLATAFORMA
------------------------------------

CREATE TABLE Plataforma (
    ID_Plataforma SERIAL PRIMARY KEY,
    Nombre_plataforma VARCHAR(255),
    Conexion VARCHAR(255)
);

------------------------------------
--   DIMENSIÓN TIEMPO
------------------------------------
CREATE TABLE Tiempo (
    ID_Tiempo SERIAL PRIMARY KEY,
    Dia INT,
    Mes INT,
    Año INT,
    Trimestre INT
);
------------------------------------
--   DIMENSIÓN DISPOSITVO
------------------------------------

CREATE TABLE Dispositivo (
    ID_Dispositivo SERIAL PRIMARY KEY,
    Tipo_dispositivo VARCHAR(255),
    Sistema_operativo VARCHAR(255)
);
------------------------------------
--   TABLA DE HECHOS CONSUMO
------------------------------------

CREATE TABLE Consumo (
    id_consumo SERIAL PRIMARY KEY,
    ID_Tiempo INT,
    ID_Ubicacion INT,
    ID_Plataforma INT,
    ID_Dispositivo INT,
    Total_conexiones INT,
    Intentos_acceso_Plataformas INT,
    FOREIGN KEY (ID_Tiempo) REFERENCES Tiempo(ID_Tiempo),
    FOREIGN KEY (ID_Ubicacion) REFERENCES Ubicacion(ID_Ubicacion),
    FOREIGN KEY (ID_Plataforma) REFERENCES Plataforma(ID_Plataforma),
    FOREIGN KEY (ID_Dispositivo) REFERENCES Dispositivo(ID_Dispositivo)
);

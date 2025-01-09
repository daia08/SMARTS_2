ALTER TABLE ubicacion
ADD CONSTRAINT ubicacion_ciudad_provincia_unique UNIQUE (Ciudad, Provincia);

ALTER TABLE plataforma
ADD CONSTRAINT plataforma_nombre_unique UNIQUE (Nombre_plataforma);

ALTER TABLE dispositivo
ADD CONSTRAINT dispositivo_tipo_unique UNIQUE (Tipo_dispositivo);

ALTER TABLE tiempo
ADD CONSTRAINT tiempo_dia_mes_ano_unique UNIQUE (Dia, Mes, Año);

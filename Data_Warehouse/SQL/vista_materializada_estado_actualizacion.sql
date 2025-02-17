CREATE MATERIALIZED VIEW vista_conexion_plataformas AS
SELECT
    t.Trimestre,
    u.Ciudad,
    u.Provincia,
    p.Nombre_plataforma,
    d.Tipo_dispositivo,
    SUM(c.total_conexiones_exitosas) + SUM(c.intentos_acceso_fallido) AS Total_intentos_conexion,
    CASE 
        WHEN SUM(c.total_conexiones_exitosas) > 5 AND SUM(c.intentos_acceso_fallido) < 5 THEN 'Actualizado'
        ELSE 'No actualizado'
    END AS Estado_Actualizacion
FROM consumo c
JOIN tiempo t ON c.id_tiempo = t.ID_Tiempo
JOIN ubicacion u ON c.id_ubicacion = u.ID_Ubicacion
JOIN plataforma p ON c.id_plataforma = p.ID_Plataforma
JOIN dispositivo d ON c.id_dispositivo = d.ID_Dispositivo
GROUP BY t.Trimestre, u.Ciudad, u.Provincia, p.Nombre_plataforma, d.Tipo_dispositivo
ORDER BY d.Tipo_dispositivo;

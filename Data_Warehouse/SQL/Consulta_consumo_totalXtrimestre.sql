SELECT
	RANK() OVER (ORDER BY SUM(c.Total_conexiones_exitosas + c.Intentos_acceso_fallido) DESC) AS Puesto,
    SUM(c.Total_conexiones_exitosas + c.Intentos_acceso_fallido) AS Total_Conexiones,
	t.Trimestre
FROM
    Consumo c
JOIN
    Tiempo t ON c.ID_Tiempo = t.ID_Tiempo
GROUP BY
    t.Trimestre
ORDER BY
    Puesto ASC;

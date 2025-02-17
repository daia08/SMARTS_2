SELECT
    d.sistema_operativo,
    SUM(c.total_conexiones_exitosas)
FROM
    Consumo c
JOIN
    Dispositivo d ON c.ID_Dispositivo = d.ID_Dispositivo
GROUP BY
    d.sistema_operativo
ORDER BY
    d.sistema_operativo ASC;

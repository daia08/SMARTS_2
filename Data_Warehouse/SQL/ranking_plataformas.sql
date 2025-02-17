SELECT 
    Nombre_plataforma,
    SUM(Total_intentos_conexion) AS Total_intentos_conexion,
    RANK() OVER (ORDER BY SUM(Total_intentos_conexion) DESC) AS Ranking_Plataforma
FROM vista_conexion_plataformas
GROUP BY Nombre_plataforma
ORDER BY Ranking_Plataforma
LIMIT 5;

SELECT t1.country_name, npop, nairports, CAST(nairports AS float)/npop*100 AS pc
FROM
 (SELECT country_name, SUM(population) AS npop
 FROM optd_por_public
 WHERE population IS NOT NULL
 GROUP BY country_name) AS t1,

 (SELECT country_name, COUNT(*) AS nairports
 FROM optd_por_public
 WHERE location_type = 'A' OR location_type = 'CA'
 GROUP BY country_name) AS t2

WHERE t1.country_name = t2.country_name
    AND npop > 0
ORDER BY pc DESC LIMIT 10;



--Lo de arriba (pero sin el ratio aeropuertos/habitantes pero con vistas. Previamente hemos creado dos vistas: population y airports con create view population AS... y pegamos la consulta entre paréntesis
SELECT population.country_name,npop,nairports
FROM population, airports
WHERE population.country_name = airports.country_name
ORDER BY npop DESC LIMIT 10;


-- Lo de antes con INNER JOIN
SELECT population.country_name,npop,nairports
FROM population 
INNER JOIN airports
ON population.country_name = airports.country_name
ORDER BY npop DESC LIMIT 10;


--Left join
SELECT airline_code_2c, name, flight_freq
FROM ref_airline_nb_of_flights AS r
LEFT OUTER JOIN optd_airlines AS o
ON o."2char_code" = r.airline_code_2c
ORDER BY flight_freq DESC LIMIT 10;

--Invertimos el orden de las tablas
SELECT airline_code_2c, name, flight_freq
FROM optd_airlines AS o
LEFT OUTER JOIN ref_airline_nb_of_flights AS r
ON o."2char_code" = r.airline_code_2c
ORDER BY flight_freq DESC LIMIT 10;

--Right join
SELECT airline_code_2c, name, flight_freq
FROM ref_airline_nb_of_flights AS r
RIGHT OUTER JOIN optd_airlines AS o
ON o."2char_code" = r.airline_code_2c
ORDER BY flight_freq DESC LIMIT 10;





--Multiples con Where
SELECT name , country_name, elevation
FROM optd_por_public
WHERE elevation > (SELECT avg(elevation) FROM optd_por_public
 WHERE elevation IS NOT NULL)
AND location_type = 'C'
LIMIT 10;

--Multiples con Having
SELECT country_name, count(*)
FROM optd_por_public
WHERE elevation > (
 SELECT avg(elevation) FROM optd_por_public
 WHERE elevation IS NOT NULL)
AND location_type = 'C'
GROUP BY country_name
HAVING COUNT(*) >= 3
LIMIT 10;

--Contar el número de ciudades de los paises que estén por encima de la -- --media de su país

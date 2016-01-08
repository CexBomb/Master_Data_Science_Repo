--Script para pruebas

-- Borra la tabla si existe
DROP TABLE IF EXISTS amigos;
-- Crea la tabla
CREATE TABLE amigos(
    nombre VARCHAR,
    edad INT,
    email VARCHAR
);

-- Algunos valores de pruebas
INSERT INTO amigos
VALUES('Juan Arevalo',35,'juan@gmail.com');

select country_name, location_type, count(*) as n from optd_por_public where country_code='ES' group by country_name, location_type ;

select country_name, count(*) as n from optd_por_public group by country_name order by n desc limit 10; 

SELECT country_name, count(*) AS n
FROM optd_por_public 
WHERE location_type = 'A' OR location_type = 'CA'
GROUP BY country_name
ORDER BY n DESC
LIMIT 10;

SELECT manufacturer, model, nb_engines
FROM optd_aircraft 
WHERE nb_engines IS NOT NULL ORDER BY nb_engines DESC
LIMIT 10;

SELECT nb_engines, count(*) FROM optd_aircraft GROUP BY nb_engines = 4;

SELECT nb_engines, count(*) FROM optd_aircraft GROUP BY nb_engines;

SELECT manufacturer, AVG(nb_engines) AS m FROM optd_aircraft WHERE nb_engines IS NOT NULL GROUP BY manufacturer ORDER BY m DESC LIMIT 10;

iata_code, name, country_name, population, elevation
FROM optd_por_public WHERE population IS NOT NULL
ORDER BY population DESC LIMIT 10;

SELECT country_name, SUM(population)
FROM optd_por_public 
WHERE population IS NOT NULL
GROUP BY country_name
ORDER BY 2 DESC LIMIT 10;

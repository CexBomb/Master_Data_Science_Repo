
SELECT c1.country_name, city_name_list, elevation, c2.el AS media FROM
(SELECT country_name, city_name_list, elevation FROM optd_por_public
WHERE elevation IS NOT NULL AND location_type = 'C') as c1,

(SELECT country_name, ROUND(AVG(elevation)) as el FROM optd_por_public
WHERE elevation IS NOT NULL AND location_type = 'C'
GROUP BY(country_name)) as c2

WHERE c1.country_name = c2.country_name AND c1.elevation > el 
ORDER BY country_name;


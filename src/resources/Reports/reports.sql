1) Amount of incidents per district:

SELECT district.neighborhood_district as District, count(*) as AmountIncidents
FROM dwh.fact_fire_incidents fire 
LEFT JOIN dwh.dim_district district 
    ON fire.sk_district = district.sk_district  
GROUP BY District

2) Amount of incidents per city

SELECT district.city as City, count(*) as AmountIncidents
FROM dwh.fact_fire_incidents fire 
LEFT JOIN dwh.dim_district district 
    ON fire.sk_district = district.sk_district  
GROUP BY District


3) Number of incidents that each Battalion assisted

SELECT bat.battalion, bat.sation_area, count(fire.id) as AmountIncidents
FROM dwh.fact_fire_incidents fire 
LEFT JOIN dwh.dim_battalion bat 
    ON fire.sk_battalion = bat.sk_battalion  
GROUP BY bat.battalion, bat.sation_area





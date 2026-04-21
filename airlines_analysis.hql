
-- 1. CRÉATION DE LA TABLE EXTERNE
-- Supprimer la table si elle existe déjà

DROP TABLE IF EXISTS flights;


-- Créer la table externe pointant vers les données HDFS
CREATE EXTERNAL TABLE flights (
    id INT COMMENT 'ID unique du vol',
    airline STRING COMMENT 'Code de la compagnie aérienne',
    flight INT COMMENT 'Numéro du vol',
    airport_from STRING COMMENT 'Code IATA aéroport de départ',
    airport_to STRING COMMENT 'Code IATA aéroport arrivée',
    day_of_week INT COMMENT 'Jour de la semaine (1=Lundi, 7=Dimanche)',
    `time` INT COMMENT 'Heure de départ prévue (format 24h)',
    length INT COMMENT 'Durée du vol en minutes',
    delay INT COMMENT 'Retard (0=non, 1=oui)'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/airlines/data/'
TBLPROPERTIES ('skip.header.line.count'='1');


-- Vérifier que la table est créée
DESCRIBE flights;


-- Afficher quelques lignes
SELECT * FROM flights LIMIT 10;


-- Compter le nombre total de vols
SELECT COUNT(*) as total_flights FROM flights;


-- 2. STATISTIQUES GLOBALES

-- Statistiques générales sur les vols
SELECT 
    COUNT(*) as total_flights,
    SUM(delay) as total_delays,
    ROUND(SUM(delay) * 100.0 / COUNT(*), 2) as delay_percentage,
    ROUND(AVG(length), 2) as avg_flight_duration,
    MIN(length) as min_duration,
    MAX(length) as max_duration
FROM flights;



-- 3. TOP 10 COMPAGNIES AVEC LE PLUS DE RETARDS
SELECT 
    airline,
    SUM(delay) as total_delays,
    COUNT(*) as total_flights,
    ROUND(SUM(delay) * 100.0 / COUNT(*), 2) as delay_rate
FROM flights
GROUP BY airline
ORDER BY total_delays DESC
LIMIT 10;



-- 4. TOP 20 AÉROPORTS DE DÉPART LES PLUS FRÉQUENTÉS
SELECT 
    airport_from,
    COUNT(*) as total_departures,
    SUM(delay) as delays_from_airport,
    ROUND(SUM(delay) * 100.0 / COUNT(*), 2) as delay_rate
FROM flights
GROUP BY airport_from
ORDER BY total_departures DESC
LIMIT 20;



-- 5. TOP 20 AÉROPORTS D'ARRIVÉE LES PLUS FRÉQUENTÉS
SELECT 
    airport_to,
    COUNT(*) as total_arrivals,
    SUM(delay) as delays_to_airport,
    ROUND(SUM(delay) * 100.0 / COUNT(*), 2) as delay_rate
FROM flights
GROUP BY airport_to
ORDER BY total_arrivals DESC
LIMIT 20;



-- 6. ANALYSE DES RETARDS PAR JOUR DE LA SEMAINE
SELECT 
    CASE day_of_week
        WHEN 1 THEN 'Lundi'
        WHEN 2 THEN 'Mardi'
        WHEN 3 THEN 'Mercredi'
        WHEN 4 THEN 'Jeudi'
        WHEN 5 THEN 'Vendredi'
        WHEN 6 THEN 'Samedi'
        WHEN 7 THEN 'Dimanche'
    END as day_name,
    day_of_week,
    COUNT(*) as total_flights,
    SUM(delay) as total_delays,
    ROUND(SUM(delay) * 100.0 / COUNT(*), 2) as delay_percentage
FROM flights
GROUP BY day_of_week
ORDER BY day_of_week;



-- 7. ANALYSE DES RETARDS PAR HEURE DE DÉPART
SELECT 
    `time` as departure_hour,
    COUNT(*) as total_flights,
    SUM(delay) as total_delays,
    ROUND(SUM(delay) * 100.0 / COUNT(*), 2) as delay_percentage
FROM flights
GROUP BY `time`
ORDER BY total_delays DESC
LIMIT 15;


-- 8. TOP 30 ROUTES AÉRIENNES LES PLUS FRÉQUENTÉES
SELECT 
    CONCAT(airport_from, '-', airport_to) as route,
    COUNT(*) as total_flights,
    SUM(delay) as total_delays,
    ROUND(AVG(length), 2) as avg_duration,
    ROUND(SUM(delay) * 100.0 / COUNT(*), 2) as delay_rate
FROM flights
GROUP BY airport_from, airport_to
ORDER BY total_flights DESC
LIMIT 30;


-- 9. TOP 20 ROUTES AVEC LE PLUS DE RETARDS
SELECT 
    CONCAT(airport_from, '-', airport_to) as route,
    COUNT(*) as total_flights,
    SUM(delay) as total_delays,
    ROUND(SUM(delay) * 100.0 / COUNT(*), 2) as delay_rate,
    ROUND(AVG(length), 2) as avg_duration
FROM flights
WHERE delay = 1
GROUP BY airport_from, airport_to
HAVING COUNT(*) >= 100  -- Filtre pour avoir un échantillon significatif
ORDER BY total_delays DESC
LIMIT 20;


-- 10. DURÉE MOYENNE DE VOL PAR COMPAGNIE
SELECT 
    airline,
    COUNT(*) as total_flights,
    ROUND(AVG(length), 2) as avg_duration,
    MIN(length) as min_duration,
    MAX(length) as max_duration
FROM flights
GROUP BY airline
ORDER BY avg_duration DESC;


-- 11. ANALYSE CROISÉE : COMPAGNIE vs JOUR
SELECT 
    airline,
    CASE day_of_week
        WHEN 1 THEN 'Lundi'
        WHEN 2 THEN 'Mardi'
        WHEN 3 THEN 'Mercredi'
        WHEN 4 THEN 'Jeudi'
        WHEN 5 THEN 'Vendredi'
        WHEN 6 THEN 'Samedi'
        WHEN 7 THEN 'Dimanche'
    END as day_name,
    COUNT(*) as total_flights,
    SUM(delay) as total_delays,
    ROUND(SUM(delay) * 100.0 / COUNT(*), 2) as delay_rate
FROM flights
WHERE airline IN ('AA', 'WN', 'US', 'CO', 'DL')  -- Top 5 compagnies
GROUP BY airline, day_of_week
ORDER BY airline, day_of_week;


-- 12. VOLS COURTS vs VOLS LONGS
SELECT 
    CASE 
        WHEN length < 60 THEN 'Très court (<1h)'
        WHEN length BETWEEN 60 AND 120 THEN 'Court (1-2h)'
        WHEN length BETWEEN 121 AND 180 THEN 'Moyen (2-3h)'
        WHEN length BETWEEN 181 AND 300 THEN 'Long (3-5h)'
        ELSE 'Très long (>5h)'
    END as duration_category,
    COUNT(*) as total_flights,
    SUM(delay) as total_delays,
    ROUND(SUM(delay) * 100.0 / COUNT(*), 2) as delay_rate
FROM flights
GROUP BY 
    CASE 
        WHEN length < 60 THEN 'Très court (<1h)'
        WHEN length BETWEEN 60 AND 120 THEN 'Court (1-2h)'
        WHEN length BETWEEN 121 AND 180 THEN 'Moyen (2-3h)'
        WHEN length BETWEEN 181 AND 300 THEN 'Long (3-5h)'
        ELSE 'Très long (>5h)'
    END
ORDER BY total_flights DESC;


-- 13. ANALYSE PAR TRANCHE HORAIRE
SELECT 
    CASE 
        WHEN `time` BETWEEN 0 AND 5 THEN 'Nuit (0h-6h)'
        WHEN `time` BETWEEN 6 AND 11 THEN 'Matin (6h-12h)'
        WHEN `time` BETWEEN 12 AND 17 THEN 'Après-midi (12h-18h)'
        ELSE 'Soir (18h-24h)'
    END as time_period,
    COUNT(*) as total_flights,
    SUM(delay) as total_delays,
    ROUND(SUM(delay) * 100.0 / COUNT(*), 2) as delay_rate
FROM flights
GROUP BY 
    CASE 
        WHEN `time` BETWEEN 0 AND 5 THEN 'Nuit (0h-6h)'
        WHEN `time` BETWEEN 6 AND 11 THEN 'Matin (6h-12h)'
        WHEN `time` BETWEEN 12 AND 17 THEN 'Après-midi (12h-18h)'
        ELSE 'Soir (18h-24h)'
    END
ORDER BY delay_rate DESC;


-- 14. AÉROPORTS LES PLUS PROBLÉMATIQUES (DÉPART)
SELECT 
    airport_from,
    COUNT(*) as total_flights,
    SUM(delay) as total_delays,
    ROUND(SUM(delay) * 100.0 / COUNT(*), 2) as delay_rate
FROM flights
GROUP BY airport_from
HAVING COUNT(*) >= 1000  -- Au moins 1000 vols
ORDER BY delay_rate DESC
LIMIT 20;


-- 15. COMPARAISON DES PERFORMANCES DES COMPAGNIES
SELECT 
    airline,
    COUNT(*) as total_flights,
    SUM(delay) as total_delays,
    ROUND(SUM(delay) * 100.0 / COUNT(*), 2) as delay_rate,
    ROUND(AVG(length), 2) as avg_duration,
    COUNT(DISTINCT airport_from) as airports_served
FROM flights
GROUP BY airline
HAVING COUNT(*) >= 5000  -- Compagnies avec au moins 5000 vols
ORDER BY delay_rate ASC;

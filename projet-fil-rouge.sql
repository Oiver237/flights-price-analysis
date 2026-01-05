//////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////

DROP DATABASE IF EXISTS FLIGHTS_DATABASE;
CREATE DATABASE FLIGHTS_DATABASE;
DROP WAREHOUSE IF EXISTS FLIGHTS_WAREHOUSE;
CREATE WAREHOUSE FLIGHTS_WAREHOUSE;
CREATE SCHEMA FLIGHTS_SCHEMA;


USE DATABASE FLIGHTS_DATABASE;
USE SCHEMA FLIGHTS_SCHEMA;
USE WAREHOUSE FLIGHTS_WAREHOUSE;


-- Storage integration
CREATE OR REPLACE STORAGE INTEGRATION S3_INT_PROJET_FIL_ROUGE
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::448479420310:role/projet-fil-rouge-iam-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://projet-fil-rouge-s3-dev/cleansed-data/');



CREATE OR REPLACE STORAGE INTEGRATION S3_INT_PROJET_FIL_ROUGE
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::448479420310:role/projet-fil-rouge-iam-role'
  STORAGE_AWS_EXTERNAL_ID = 'QUB67612_SFCRole=6_eGR4joA0GMEczDHgNAbCnrxkH8s='
  STORAGE_ALLOWED_LOCATIONS = ('s3://projet-fil-rouge-s3-dev/cleansed-data/');

DESC INTEGRATION S3_INT_PROJET_FIL_ROUGE;


CREATE OR REPLACE FILE FORMAT FF_PARQUET
TYPE = PARQUET
COMPRESSION = SNAPPY;


-- Staging area 
CREATE OR REPLACE STAGE STG_FLIGHTS
  STORAGE_INTEGRATION = S3_INT_PROJET_FIL_ROUGE
  URL = 's3://projet-fil-rouge-s3-dev/cleansed-data/flights/'
  FILE_FORMAT = FF_PARQUET;

CREATE OR REPLACE STAGE STG_LAYOVERS
  STORAGE_INTEGRATION = S3_INT_PROJET_FIL_ROUGE
  URL = 's3://projet-fil-rouge-s3-dev/cleansed-data/layovers/'
  FILE_FORMAT = FF_PARQUET;

CREATE OR REPLACE STAGE STG_PRICE_HISTORY
  STORAGE_INTEGRATION = S3_INT_PROJET_FIL_ROUGE
  URL = 's3://projet-fil-rouge-s3-dev/cleansed-data/price_history/'
  FILE_FORMAT = FF_PARQUET;

CREATE OR REPLACE STAGE STG_SEARCH_PARAMETERS
  STORAGE_INTEGRATION = S3_INT_PROJET_FIL_ROUGE
  URL = 's3://projet-fil-rouge-s3-dev/cleansed-data/search_parameters/'
  FILE_FORMAT = FF_PARQUET;

//////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////


-- FLIGHTS
CREATE OR REPLACE EXTERNAL TABLE EXT_FLIGHTS (
  search_id                 STRING        AS ($1:search_id::string),
  departure_token           STRING        AS ($1:departure_token::string),
  leg_number                NUMBER        AS ($1:leg_number::int),
  departure_airport_name    STRING        AS ($1:departure_airport_name::string),
  departure_airport_id      STRING        AS ($1:departure_airport_id::string),
  arrival_airport_name      STRING        AS ($1:arrival_airport_name::string),
  arrival_airport_id        STRING        AS ($1:arrival_airport_id::string),
  flight_duration           NUMBER        AS ($1:flight_duration::bigint),
  airplane_type             STRING        AS ($1:airplane_type::string),
  airline                   STRING        AS ($1:airline::string),
  airline_logo              STRING        AS ($1:airline_logo::string),
  travel_class              STRING        AS ($1:travel_class::string),
  flight_number             STRING        AS ($1:flight_number::string),
  legroom                   STRING        AS ($1:legroom::string),
  extensions                VARIANT       AS ($1:extensions),           -- array<string>
  often_delayed             BOOLEAN       AS ($1:often_delayed::boolean),
  total_itinerary_duration  NUMBER        AS ($1:total_itinerary_duration::bigint),
  itinerary_price           NUMBER        AS ($1:itinerary_price::bigint),
  carbon_emissions          NUMBER        AS ($1:carbon_emissions::bigint),
  typical_carbon_emissions  NUMBER        AS ($1:typical_carbon_emissions::bigint),
  carbon_difference_percent NUMBER        AS ($1:carbon_difference_percent::bigint),
  flight_type               STRING        AS ($1:flight_type::string),
  trip_type                 STRING        AS ($1:trip_type::string),
  departure_datetime        TIMESTAMP_NTZ AS ($1:departure_datetime::timestamp),
  arrival_datetime          TIMESTAMP_NTZ AS ($1:arrival_datetime::timestamp),
  price_euros               NUMBER(10,2)  AS ($1:price_euros::decimal(10,2)),
  search_timestamp          TIMESTAMP_NTZ AS ($1:search_timestamp::timestamp),
  year                      NUMBER        AS ($1:year::int),
  month                     NUMBER        AS ($1:month::int)
)
LOCATION=@STG_FLIGHTS
FILE_FORMAT=FF_PARQUET;

-- LAYOVERS
CREATE OR REPLACE EXTERNAL TABLE EXT_LAYOVERS (
  search_id              STRING        AS ($1:search_id::string),
  departure_token        STRING        AS ($1:departure_token::string),
  layover_number         NUMBER        AS ($1:layover_number::int),
  layover_duration       NUMBER        AS ($1:layover_duration::bigint),
  layover_airport_name   STRING        AS ($1:layover_airport_name::string),
  layover_airport_id     STRING        AS ($1:layover_airport_id::string),
  is_overnight           BOOLEAN       AS ($1:is_overnight::boolean),
  flight_type            STRING        AS ($1:flight_type::string)
)
LOCATION=@STG_LAYOVERS
FILE_FORMAT=FF_PARQUET;

-- PRICE HISTORY
CREATE OR REPLACE EXTERNAL TABLE EXT_PRICE_HISTORY (
  search_id   STRING        AS ($1:search_id::string),
  timestamp   NUMBER        AS ($1:timestamp::bigint),
  price       NUMBER        AS ($1:price::bigint),
  date_time   STRING        AS ($1:date_time::string)
)
LOCATION=@STG_PRICE_HISTORY
FILE_FORMAT=FF_PARQUET;

-- SEARCH PARAMETERS
CREATE OR REPLACE EXTERNAL TABLE EXT_SEARCH_PARAMETERS (
  search_id   STRING        AS ($1:search_id::string),
  timestamp   NUMBER        AS ($1:timestamp::bigint),
  price       NUMBER        AS ($1:price::bigint),
  date_time   STRING        AS ($1:date_time::string)
)
LOCATION=@STG_SEARCH_PARAMETERS
FILE_FORMAT=FF_PARQUET;

//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////

-- Calendrier : DIM_DATE
CREATE OR REPLACE TABLE DIM_DATE (
  DATE_KEY     NUMBER(8) PRIMARY KEY,  -- yyyymmdd
  FULL_DATE    DATE,
  DAY_OF_WEEK  NUMBER,                 -- 1=Mon ... 7=Sun
  DAY_NAME     STRING,
  DAY_OF_MONTH NUMBER,
  WEEK_OF_YEAR NUMBER,
  MONTH_NUM    NUMBER,
  MONTH_NAME   STRING,
  QUARTER_NUM  NUMBER,
  YEAR_NUM     NUMBER,
  IS_WEEKEND   BOOLEAN
);

-- Heure de la journée : DIM_TIME
CREATE OR REPLACE TABLE DIM_TIME (
  TIME_KEY      NUMBER(6) PRIMARY KEY, -- hhmmss
  HOUR_NUM      NUMBER,
  MINUTE_NUM    NUMBER,
  SECOND_NUM    NUMBER,
  MINUTE_OF_DAY NUMBER
);

-- Aéroport : DIM_AIRPORT
CREATE OR REPLACE TABLE DIM_AIRPORT (
  AIRPORT_KEY   NUMBER AUTOINCREMENT PRIMARY KEY,
  AIRPORT_ID    STRING,     -- *_airport_id comme clé naturelle
  AIRPORT_NAME  STRING,
  EFFECTIVE_TS  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
  END_TS        TIMESTAMP_NTZ,
  IS_CURRENT    BOOLEAN DEFAULT TRUE
);

-- Compagnie : DIM_AIRLINE
CREATE OR REPLACE TABLE DIM_AIRLINE (
  AIRLINE_KEY   NUMBER AUTOINCREMENT PRIMARY KEY,
  AIRLINE_NAME  STRING,
  EFFECTIVE_TS  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
  END_TS        TIMESTAMP_NTZ,
  IS_CURRENT    BOOLEAN DEFAULT TRUE
);

-- Type d’appareil : DIM_AIRCRAFT
CREATE OR REPLACE TABLE DIM_AIRCRAFT (
  AIRCRAFT_KEY  NUMBER AUTOINCREMENT PRIMARY KEY,
  AIRCRAFT_TYPE STRING
);

-- Classe cabine : DIM_CABIN
CREATE OR REPLACE TABLE DIM_CABIN (
  CABIN_KEY   NUMBER AUTOINCREMENT PRIMARY KEY,
  CABIN_NAME  STRING   -- travel_class
);

-- Contexte de recherche : DIM_SEARCH
CREATE OR REPLACE TABLE DIM_SEARCH (
  SEARCH_KEY  NUMBER AUTOINCREMENT PRIMARY KEY,
  SEARCH_ID   STRING UNIQUE,
  CREATED_TS  TIMESTAMP_NTZ  -- dérivé de timestamp/date_time des search_parameters
);




-- Fait offre itinéraire
CREATE OR REPLACE TABLE FACT_ITINERARY_OFFER (
  ITINERARY_OFFER_KEY NUMBER AUTOINCREMENT PRIMARY KEY,
  SEARCH_ID           STRING,           -- dégénérée
  DEPARTURE_TOKEN     STRING,           -- identifiant d’itinéraire

  -- FKs
  SEARCH_KEY            NUMBER,
  DEPARTURE_AIRPORT_KEY NUMBER,
  ARRIVAL_AIRPORT_KEY   NUMBER,
  AIRLINE_KEY           NUMBER,
  CABIN_KEY             NUMBER,
  DEPARTURE_DATE_KEY    NUMBER,
  ARRIVAL_DATE_KEY      NUMBER,
  DEPARTURE_TIME_KEY    NUMBER,
  ARRIVAL_TIME_KEY      NUMBER,

  -- Mesures / attributs
  TOTAL_ITINERARY_DURATION NUMBER,
  STOPS_COUNT              NUMBER,
  PRICE_EUR                NUMBER(18,2),
  ITINERARY_PRICE_RAW      NUMBER,
  FLIGHT_TYPE              STRING,
  TRIP_TYPE                STRING,
  OFFER_CREATED_TS         TIMESTAMP_NTZ
);

-- Fait leg (tronçon)
CREATE OR REPLACE TABLE FACT_FLIGHT_LEG (
  FLIGHT_LEG_KEY        NUMBER AUTOINCREMENT PRIMARY KEY,
  ITINERARY_OFFER_KEY   NUMBER,
  LEG_NUMBER            NUMBER,
  FLIGHT_NUMBER         STRING,
  AIRCRAFT_KEY          NUMBER,
  AIRLINE_KEY           NUMBER,
  DEPARTURE_AIRPORT_KEY NUMBER,
  ARRIVAL_AIRPORT_KEY   NUMBER,
  DEPARTURE_DATE_KEY    NUMBER,
  ARRIVAL_DATE_KEY      NUMBER,
  DEPARTURE_TIME_KEY    NUMBER,
  ARRIVAL_TIME_KEY      NUMBER,
  CABIN_KEY             NUMBER,

  FLIGHT_DURATION       NUMBER,
  OFTEN_DELAYED         BOOLEAN,
  LEGROOM               STRING,
  CARBON_EMISSIONS      NUMBER,
  TYPICAL_CARBON_EMISSIONS NUMBER,
  CARBON_DIFF_PERCENT   NUMBER
);

-- Bridge des escales
CREATE OR REPLACE TABLE BRIDGE_ITINERARY_LAYOVER (
  ITINERARY_OFFER_KEY  NUMBER,
  LAYOVER_SEQ          NUMBER,  -- layover_number
  LAYOVER_AIRPORT_KEY  NUMBER,
  LAYOVER_DURATION     NUMBER,
  IS_OVERNIGHT         BOOLEAN
);

-- Historique de prix
CREATE OR REPLACE TABLE FACT_PRICE_SNAPSHOT (
  PRICE_SNAPSHOT_KEY   NUMBER AUTOINCREMENT PRIMARY KEY,
  ITINERARY_OFFER_KEY  NUMBER,
  SNAPSHOT_DATE_KEY    NUMBER,
  SNAPSHOT_TIME_KEY    NUMBER,
  PRICE_AMOUNT         NUMBER(18,2),
  SNAPSHOT_TS          TIMESTAMP_NTZ,
  SOURCE               STRING       -- 'PRICE_HISTORY' ou 'SEARCH_PARAMETERS'
);



-- Legs détaillés : réutilisés pour l’agrégation itinéraire
CREATE OR REPLACE VIEW VW_STG_FLIGHT_LEGS AS
SELECT
  search_id,
  departure_token,
  leg_number,
  flight_number,
  airplane_type,
  airline,
  travel_class,
  departure_airport_id,
  departure_airport_name,
  arrival_airport_id,
  arrival_airport_name,
  departure_datetime,
  arrival_datetime,
  flight_duration,
  often_delayed,
  legroom,
  carbon_emissions,
  typical_carbon_emissions,
  carbon_difference_percent,
  trip_type,
  flight_type,
  price_euros,
  itinerary_price,
  total_itinerary_duration,
  search_timestamp
FROM EXT_FLIGHTS;

-- Itinéraire (offre) : agrégation des legs par (search_id, departure_token)
CREATE OR REPLACE VIEW VW_STG_ITINERARIES AS
WITH ordered AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY search_id, departure_token ORDER BY leg_number ASC)  AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY search_id, departure_token ORDER BY leg_number DESC) AS rn_desc,
    COUNT(*)    OVER (PARTITION BY search_id, departure_token) AS leg_cnt
  FROM VW_STG_FLIGHT_LEGS
)
SELECT
  search_id,
  departure_token,
  ANY_VALUE(trip_type)                      AS trip_type,
  ANY_VALUE(flight_type)                    AS flight_type,
  ANY_VALUE(total_itinerary_duration)       AS total_itinerary_duration,
  ANY_VALUE(price_euros)                    AS price_eur,
  ANY_VALUE(itinerary_price)                AS itinerary_price_raw,
  (MAX(leg_cnt) - 1)                        AS stops_count,
  MAX(CASE WHEN rn_asc=1  THEN departure_airport_id   END) AS origin_airport_id,
  MAX(CASE WHEN rn_asc=1  THEN departure_airport_name END) AS origin_airport_name,
  MAX(CASE WHEN rn_asc=1  THEN departure_datetime     END) AS origin_departure_ts,
  MAX(CASE WHEN rn_desc=1 THEN arrival_airport_id     END) AS dest_airport_id,
  MAX(CASE WHEN rn_desc=1 THEN arrival_airport_name   END) AS dest_airport_name,
  MAX(CASE WHEN rn_desc=1 THEN arrival_datetime       END) AS dest_arrival_ts,
  MAX(CASE WHEN rn_asc=1  THEN airline                END) AS airline_name,
  MAX(CASE WHEN rn_asc=1  THEN travel_class           END) AS cabin_name,
  ANY_VALUE(search_timestamp)                          AS offer_created_ts
FROM ordered
GROUP BY search_id, departure_token;

-- Layovers (bridge-ready)
CREATE OR REPLACE VIEW VW_STG_LAYOVERS AS
SELECT
  search_id,
  departure_token,
  layover_number      AS layover_seq,
  layover_duration,
  layover_airport_id,
  layover_airport_name,
  is_overnight
FROM EXT_LAYOVERS;

-- Price history
CREATE OR REPLACE VIEW VW_STG_PRICE_HISTORY AS
SELECT
  search_id,
  TO_TIMESTAMP_NTZ(timestamp)         AS snapshot_ts,
  price::NUMBER(18,2)                 AS price_amount,
  'PRICE_HISTORY'                     AS source
FROM EXT_PRICE_HISTORY;


-- Search parameters (première observation)
CREATE OR REPLACE VIEW VW_STG_SEARCH_PARAMETERS AS
SELECT
  search_id,
  COALESCE(TRY_TO_TIMESTAMP_NTZ(date_time),
           TO_TIMESTAMP_NTZ(timestamp))             AS snapshot_ts,
  price::NUMBER(18,2)                               AS price_amount,
  'SEARCH_PARAMETERS'                               AS source
FROM EXT_SEARCH_PARAMETERS;




-- DIM_DATE pour ~5 ans (ajuste le début/fin à tes besoins)
INSERT INTO DIM_DATE
SELECT
  TO_NUMBER(TO_CHAR(DATEADD('day', seq4(), '2023-01-01'::DATE), 'YYYYMMDD')) AS DATE_KEY,
  DATEADD('day', seq4(), '2023-01-01'::DATE) AS FULL_DATE,
  DAYOFWEEKISO(FULL_DATE) AS DAY_OF_WEEK,
  TO_CHAR(FULL_DATE, 'DY') AS DAY_NAME,
  DAY(FULL_DATE) AS DAY_OF_MONTH,
  WEEKISO(FULL_DATE) AS WEEK_OF_YEAR,
  MONTH(FULL_DATE) AS MONTH_NUM,
  TO_CHAR(FULL_DATE, 'MON') AS MONTH_NAME,
  QUARTER(FULL_DATE) AS QUARTER_NUM,
  YEAR(FULL_DATE) AS YEAR_NUM,
  DAYOFWEEKISO(FULL_DATE) IN (6,7) AS IS_WEEKEND
FROM TABLE(GENERATOR(ROWCOUNT => 1826))
WHERE NOT EXISTS (SELECT 1 FROM DIM_DATE);

-- DIM_TIME (chaque minute 00:00 -> 23:59)
INSERT INTO DIM_TIME
SELECT
  TO_NUMBER(LPAD(TO_CHAR(FLOOR(seq4()/60)),2,'0') || LPAD(TO_CHAR(MOD(seq4(),60)),2,'0') || '00') AS TIME_KEY,
  FLOOR(seq4()/60)  AS HOUR_NUM,
  MOD(seq4(),60)    AS MINUTE_NUM,
  0                 AS SECOND_NUM,
  FLOOR(seq4()/60)*60 + MOD(seq4(),60) AS MINUTE_OF_DAY
FROM TABLE(GENERATOR(ROWCOUNT => 1440))
WHERE NOT EXISTS (SELECT 1 FROM DIM_TIME);




-- Aéroports : origine
MERGE INTO DIM_AIRPORT D
USING (
  SELECT DISTINCT origin_airport_id AS AIRPORT_ID, origin_airport_name AS AIRPORT_NAME
  FROM VW_STG_ITINERARIES
  WHERE origin_airport_id IS NOT NULL
) S
ON D.AIRPORT_ID = S.AIRPORT_ID AND D.IS_CURRENT = TRUE
WHEN MATCHED AND NVL(D.AIRPORT_NAME,'') <> NVL(S.AIRPORT_NAME,'') THEN
  UPDATE SET END_TS = CURRENT_TIMESTAMP, IS_CURRENT = FALSE
WHEN NOT MATCHED THEN
  INSERT (AIRPORT_ID, AIRPORT_NAME) VALUES (S.AIRPORT_ID, S.AIRPORT_NAME);

-- Aéroports : destination
MERGE INTO DIM_AIRPORT D
USING (
  SELECT DISTINCT dest_airport_id AS AIRPORT_ID, dest_airport_name AS AIRPORT_NAME
  FROM VW_STG_ITINERARIES
  WHERE dest_airport_id IS NOT NULL
) S
ON D.AIRPORT_ID = S.AIRPORT_ID AND D.IS_CURRENT = TRUE
WHEN MATCHED AND NVL(D.AIRPORT_NAME,'') <> NVL(S.AIRPORT_NAME,'') THEN
  UPDATE SET END_TS = CURRENT_TIMESTAMP, IS_CURRENT = FALSE
WHEN NOT MATCHED THEN
  INSERT (AIRPORT_ID, AIRPORT_NAME) VALUES (S.AIRPORT_ID, S.AIRPORT_NAME);

-- Compagnies
MERGE INTO DIM_AIRLINE D
USING (SELECT DISTINCT airline_name AS AIRLINE_NAME FROM VW_STG_ITINERARIES WHERE airline_name IS NOT NULL) S
ON UPPER(D.AIRLINE_NAME) = UPPER(S.AIRLINE_NAME) AND D.IS_CURRENT = TRUE
WHEN NOT MATCHED THEN
  INSERT (AIRLINE_NAME) VALUES (S.AIRLINE_NAME);

-- Types d’appareils
MERGE INTO DIM_AIRCRAFT D
USING (SELECT DISTINCT airplane_type AS AIRCRAFT_TYPE FROM VW_STG_FLIGHT_LEGS WHERE airplane_type IS NOT NULL) S
ON UPPER(D.AIRCRAFT_TYPE) = UPPER(S.AIRCRAFT_TYPE)
WHEN NOT MATCHED THEN
  INSERT (AIRCRAFT_TYPE) VALUES (S.AIRCRAFT_TYPE);

-- Cabines
MERGE INTO DIM_CABIN D
USING (SELECT DISTINCT travel_class AS CABIN_NAME FROM VW_STG_FLIGHT_LEGS WHERE travel_class IS NOT NULL) S
ON UPPER(D.CABIN_NAME) = UPPER(S.CABIN_NAME)
WHEN NOT MATCHED THEN
  INSERT (CABIN_NAME) VALUES (S.CABIN_NAME);

-- Search (id + horodatage “création”)
MERGE INTO DIM_SEARCH D
USING (
  SELECT search_id,
         MIN(snapshot_ts) AS created_ts
  FROM (
    SELECT search_id, snapshot_ts FROM VW_STG_SEARCH_PARAMETERS
    UNION ALL
    SELECT search_id, snapshot_ts FROM VW_STG_PRICE_HISTORY
  ) u
  GROUP BY search_id
) S
ON D.SEARCH_ID = S.SEARCH_ID
WHEN MATCHED THEN UPDATE SET CREATED_TS = S.created_ts
WHEN NOT MATCHED THEN INSERT (SEARCH_ID, CREATED_TS) VALUES (S.search_id, S.created_ts);

//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////

-- FACT_ITINERARY_OFFER
INSERT INTO FACT_ITINERARY_OFFER (
  SEARCH_ID, DEPARTURE_TOKEN,
  SEARCH_KEY,
  DEPARTURE_AIRPORT_KEY, ARRIVAL_AIRPORT_KEY,
  AIRLINE_KEY, CABIN_KEY,
  DEPARTURE_DATE_KEY, ARRIVAL_DATE_KEY,
  DEPARTURE_TIME_KEY, ARRIVAL_TIME_KEY,
  TOTAL_ITINERARY_DURATION, STOPS_COUNT,
  PRICE_EUR, ITINERARY_PRICE_RAW,
  FLIGHT_TYPE, TRIP_TYPE, OFFER_CREATED_TS
)
SELECT
  I.search_id,
  I.departure_token,
  DS.SEARCH_KEY,
  APD.AIRPORT_KEY,
  APA.AIRPORT_KEY,
  AL.AIRLINE_KEY,
  CAB.CABIN_KEY,
  TO_NUMBER(TO_CHAR(I.origin_departure_ts, 'YYYYMMDD')) AS DEPARTURE_DATE_KEY,
  TO_NUMBER(TO_CHAR(I.dest_arrival_ts,     'YYYYMMDD')) AS ARRIVAL_DATE_KEY,
  TO_NUMBER(TO_CHAR(I.origin_departure_ts, 'HH24MISS')) AS DEPARTURE_TIME_KEY,
  TO_NUMBER(TO_CHAR(I.dest_arrival_ts,     'HH24MISS')) AS ARRIVAL_TIME_KEY,
  I.total_itinerary_duration,
  I.stops_count,
  I.price_eur,
  I.itinerary_price_raw,
  I.flight_type,
  I.trip_type,
  I.offer_created_ts
FROM VW_STG_ITINERARIES I
LEFT JOIN DIM_SEARCH  DS ON DS.SEARCH_ID = I.search_id
LEFT JOIN DIM_AIRPORT APD ON APD.AIRPORT_ID = I.origin_airport_id AND APD.IS_CURRENT = TRUE
LEFT JOIN DIM_AIRPORT APA ON APA.AIRPORT_ID = I.dest_airport_id   AND APA.IS_CURRENT = TRUE
LEFT JOIN DIM_AIRLINE AL   ON UPPER(AL.AIRLINE_NAME) = UPPER(I.airline_name) AND AL.IS_CURRENT = TRUE
LEFT JOIN DIM_CABIN   CAB  ON UPPER(CAB.CABIN_NAME)  = UPPER(I.cabin_name);



--FACT_FLIGHT_LEG
INSERT INTO FACT_FLIGHT_LEG (
  ITINERARY_OFFER_KEY,
  LEG_NUMBER, FLIGHT_NUMBER,
  AIRCRAFT_KEY, AIRLINE_KEY,
  DEPARTURE_AIRPORT_KEY, ARRIVAL_AIRPORT_KEY,
  DEPARTURE_DATE_KEY, ARRIVAL_DATE_KEY,
  DEPARTURE_TIME_KEY, ARRIVAL_TIME_KEY,
  CABIN_KEY,
  FLIGHT_DURATION, OFTEN_DELAYED, LEGROOM,
  CARBON_EMISSIONS, TYPICAL_CARBON_EMISSIONS, CARBON_DIFF_PERCENT
)
SELECT
  FIO.ITINERARY_OFFER_KEY,
  L.leg_number,
  L.flight_number,
  AC.AIRCRAFT_KEY,
  AL.AIRLINE_KEY,
  APD.AIRPORT_KEY,
  APA.AIRPORT_KEY,
  TO_NUMBER(TO_CHAR(L.departure_datetime, 'YYYYMMDD')) AS DEPARTURE_DATE_KEY,
  TO_NUMBER(TO_CHAR(L.arrival_datetime,   'YYYYMMDD')) AS ARRIVAL_DATE_KEY,
  TO_NUMBER(TO_CHAR(L.departure_datetime, 'HH24MISS')) AS DEPARTURE_TIME_KEY,
  TO_NUMBER(TO_CHAR(L.arrival_datetime,   'HH24MISS')) AS ARRIVAL_TIME_KEY,
  CAB.CABIN_KEY,
  L.flight_duration,
  L.often_delayed,
  L.legroom,
  L.carbon_emissions,
  L.typical_carbon_emissions,
  L.carbon_difference_percent
FROM VW_STG_FLIGHT_LEGS L
JOIN FACT_ITINERARY_OFFER FIO
  ON FIO.SEARCH_ID = L.search_id AND FIO.DEPARTURE_TOKEN = L.departure_token
LEFT JOIN DIM_AIRCRAFT AC ON UPPER(AC.AIRCRAFT_TYPE) = UPPER(L.airplane_type)
LEFT JOIN DIM_AIRLINE  AL ON UPPER(AL.AIRLINE_NAME)  = UPPER(L.airline) AND AL.IS_CURRENT = TRUE
LEFT JOIN DIM_AIRPORT  APD ON APD.AIRPORT_ID = L.departure_airport_id AND APD.IS_CURRENT = TRUE
LEFT JOIN DIM_AIRPORT  APA ON APA.AIRPORT_ID = L.arrival_airport_id   AND APA.IS_CURRENT = TRUE
LEFT JOIN DIM_CABIN    CAB ON UPPER(CAB.CABIN_NAME)  = UPPER(L.travel_class);



--BRIDGE_ITINERARY_LAYOVER
INSERT INTO BRIDGE_ITINERARY_LAYOVER (
  ITINERARY_OFFER_KEY, LAYOVER_SEQ, LAYOVER_AIRPORT_KEY, LAYOVER_DURATION, IS_OVERNIGHT
)
SELECT
  FIO.ITINERARY_OFFER_KEY,
  L.layover_seq,
  AP.AIRPORT_KEY,
  L.layover_duration,
  L.is_overnight
FROM VW_STG_LAYOVERS L
JOIN FACT_ITINERARY_OFFER FIO
  ON FIO.SEARCH_ID = L.search_id AND FIO.DEPARTURE_TOKEN = L.departure_token
LEFT JOIN DIM_AIRPORT AP
  ON AP.AIRPORT_ID = L.layover_airport_id AND AP.IS_CURRENT = TRUE;


  
-- Depuis PRICE_HISTORY
INSERT INTO FACT_PRICE_SNAPSHOT (
  ITINERARY_OFFER_KEY, SNAPSHOT_DATE_KEY, SNAPSHOT_TIME_KEY, PRICE_AMOUNT, SNAPSHOT_TS, SOURCE
)
SELECT
  FIO.ITINERARY_OFFER_KEY,
  TO_NUMBER(TO_CHAR(P.snapshot_ts, 'YYYYMMDD')) AS SNAPSHOT_DATE_KEY,
  TO_NUMBER(TO_CHAR(P.snapshot_ts, 'HH24MISS')) AS SNAPSHOT_TIME_KEY,
  P.price_amount,
  P.snapshot_ts,
  P.source
FROM VW_STG_PRICE_HISTORY P
JOIN FACT_ITINERARY_OFFER FIO ON FIO.SEARCH_ID = P.search_id;

-- Première observation depuis SEARCH_PARAMETERS
INSERT INTO FACT_PRICE_SNAPSHOT (
  ITINERARY_OFFER_KEY, SNAPSHOT_DATE_KEY, SNAPSHOT_TIME_KEY, PRICE_AMOUNT, SNAPSHOT_TS, SOURCE
)
SELECT
  FIO.ITINERARY_OFFER_KEY,
  TO_NUMBER(TO_CHAR(SP.snapshot_ts, 'YYYYMMDD')) AS SNAPSHOT_DATE_KEY,
  TO_NUMBER(TO_CHAR(SP.snapshot_ts, 'HH24MISS')) AS SNAPSHOT_TIME_KEY,
  SP.price_amount,
  SP.snapshot_ts,
  SP.source
FROM VW_STG_SEARCH_PARAMETERS SP
JOIN FACT_ITINERARY_OFFER FIO ON FIO.SEARCH_ID = SP.search_id;



//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////


-- Prix & durée moyennes par compagnie pour les itinéraires chargés
SELECT
  AL.AIRLINE_NAME,
  AVG(F.PRICE_EUR) AS AVG_PRICE_EUR,
  AVG(F.TOTAL_ITINERARY_DURATION) AS AVG_DURATION_MIN
FROM FACT_ITINERARY_OFFER F
JOIN DIM_AIRLINE AL ON AL.AIRLINE_KEY = F.AIRLINE_KEY AND AL.IS_CURRENT = TRUE
GROUP BY 1
ORDER BY AVG_PRICE_EUR ASC;


-- Impact des escales
SELECT STOPS_COUNT, COUNT(*) AS NB_ITINS,
       AVG(PRICE_EUR) AS AVG_PRICE, AVG(TOTAL_ITINERARY_DURATION) AS AVG_DURATION
FROM FACT_ITINERARY_OFFER
GROUP BY STOPS_COUNT
ORDER BY STOPS_COUNT;



-- Paramètres (exemple) : Paris-CDG -> Yaoundé-NSI, Octobre 2025
WITH params AS (
  SELECT 'CDG' AS ORIGIN_ID, 'NSI' AS DEST_ID, 20251001 AS D1, 20251031 AS D2
)
SELECT
  AL.AIRLINE_NAME,
  F.SEARCH_ID,
  F.DEPARTURE_TOKEN,
  F.PRICE_EUR,
  F.TOTAL_ITINERARY_DURATION AS DURATION_MIN,
  ROUND(F.PRICE_EUR / NULLIF(F.TOTAL_ITINERARY_DURATION,0), 2) AS EUR_PER_MIN,
  F.STOPS_COUNT
FROM FACT_ITINERARY_OFFER F
JOIN DIM_AIRPORT APD 
  ON APD.AIRPORT_KEY = F.DEPARTURE_AIRPORT_KEY 
 AND APD.IS_CURRENT = TRUE
JOIN DIM_AIRPORT APA 
  ON APA.AIRPORT_KEY = F.ARRIVAL_AIRPORT_KEY   
 AND APA.IS_CURRENT = TRUE
JOIN DIM_AIRLINE AL  
  ON AL.AIRLINE_KEY  = F.AIRLINE_KEY           
 AND AL.IS_CURRENT = TRUE
JOIN params p ON 1=1
WHERE APD.AIRPORT_ID = p.ORIGIN_ID
  AND APA.AIRPORT_ID = p.DEST_ID
  AND F.DEPARTURE_DATE_KEY BETWEEN p.D1 AND p.D2
ORDER BY
  EUR_PER_MIN ASC,
  F.STOPS_COUNT ASC;



-- Direct uniquement (STOPS_COUNT = 0)
SELECT
  F.SEARCH_ID, F.DEPARTURE_TOKEN,
  APD.AIRPORT_ID AS ORIGIN, APA.AIRPORT_ID AS DEST,
  AL.AIRLINE_NAME,
  F.TOTAL_ITINERARY_DURATION AS DURATION_MIN,
  F.PRICE_EUR
FROM FACT_ITINERARY_OFFER F
JOIN DIM_AIRPORT APD ON APD.AIRPORT_KEY = F.DEPARTURE_AIRPORT_KEY AND APD.IS_CURRENT = TRUE
JOIN DIM_AIRPORT APA ON APA.AIRPORT_KEY = F.ARRIVAL_AIRPORT_KEY   AND APA.IS_CURRENT = TRUE
LEFT JOIN DIM_AIRLINE AL  ON AL.AIRLINE_KEY  = F.AIRLINE_KEY       AND AL.IS_CURRENT = TRUE
WHERE F.STOPS_COUNT = 0
  AND F.DEPARTURE_DATE_KEY BETWEEN 20251001 AND 20251031
ORDER BY DURATION_MIN ASC, PRICE_EUR ASC



WITH CO2 AS (
  SELECT
    L.ITINERARY_OFFER_KEY,
    SUM(NVL(L.CARBON_EMISSIONS, 0)) AS TOTAL_CO2
  FROM FACT_FLIGHT_LEG L
  GROUP BY L.ITINERARY_OFFER_KEY
)
SELECT
  F.SEARCH_ID, F.DEPARTURE_TOKEN,
  AL.AIRLINE_NAME,
  F.PRICE_EUR,
  F.TOTAL_ITINERARY_DURATION AS DURATION_MIN,
  C.TOTAL_CO2
FROM FACT_ITINERARY_OFFER F
JOIN CO2 C ON C.ITINERARY_OFFER_KEY = F.ITINERARY_OFFER_KEY
LEFT JOIN DIM_AIRLINE AL ON AL.AIRLINE_KEY = F.AIRLINE_KEY AND AL.IS_CURRENT = TRUE
WHERE F.DEPARTURE_DATE_KEY BETWEEN 20251001 AND 20251031
ORDER BY C.TOTAL_CO2 ASC, F.PRICE_EUR ASC
LIMIT 20;




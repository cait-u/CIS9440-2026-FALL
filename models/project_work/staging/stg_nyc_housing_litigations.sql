-- Clean and standardize HPD Housing Litigations
-- Source: NYC Open Data 59kj-x8nc (HPD Housing Litigation Division cases)
-- One row per litigation (litigation_id)
-- Conformed column names shared with the 311 staging model:
--   created_date, status, zip, borough, street_name, incident_address, latitude, longitude

WITH source AS (
    SELECT * FROM {{ source('raw', 'source_housing_litigations') }}
), -- Easier to refer to the dbt reference to a long name table this way

cleaned AS (
    SELECT
        -- Identifiers
        CAST(litigationid AS STRING) AS litigation_id,
        CAST(buildingid AS STRING) AS building_id,
        CAST(bin AS STRING) AS bin,
        CAST(block AS STRING) AS block,
        CAST(lot AS STRING) AS lot,
        CAST(bbl AS STRING) AS bbl,  -- borough-block-lot key, handy for joining to PLUTO

        -- Date/Time (created_date conforms with 311)
        -- Handles caseopendate whether it was loaded as a timestamp or as MM/DD/YYYY text
        COALESCE(
            SAFE_CAST(caseopendate AS TIMESTAMP),
            SAFE.PARSE_TIMESTAMP('%m/%d/%Y', CAST(caseopendate AS STRING))
        ) AS created_date,
        COALESCE(
            SAFE_CAST(findingdate AS TIMESTAMP),
            SAFE.PARSE_TIMESTAMP('%m/%d/%Y', CAST(findingdate AS STRING))
        ) AS harassment_finding_date,

        -- Case details
        UPPER(TRIM(CAST(casetype AS STRING))) AS case_type,
        UPPER(TRIM(CAST(casestatus AS STRING))) AS status,  -- conforms with 311
        UPPER(TRIM(CAST(casejudgement AS STRING))) AS case_judgement,
        UPPER(TRIM(CAST(findingofharassment AS STRING))) AS harassment_finding,
        UPPER(TRIM(CAST(respondent AS STRING))) AS respondent,
        -- penalty is stored as text in the source, so strip $ and commas before casting
        SAFE_CAST(REGEXP_REPLACE(CAST(penalty AS STRING), r'[^0-9.]', '') AS FLOAT64) AS penalty_usd,

        -- Location - clean zip code
        CASE
            WHEN UPPER(TRIM(CAST(zip AS STRING))) IN ('N/A', 'NA', 'NONE', '') THEN NULL
            WHEN REGEXP_CONTAINS(TRIM(CAST(zip AS STRING)), r'^\d{5}(-?\d{4})?$')
                THEN LEFT(TRIM(CAST(zip AS STRING)), 5)  -- keep 5 digits to match 311
            ELSE NULL
        END AS zip,

        -- Location - standardized borough to match 311 values
        -- Source stores a numeric code (1-5); text names are handled too in case that changes
        CASE
            WHEN UPPER(TRIM(CAST(boroid AS STRING))) IN ('1', 'MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
            WHEN UPPER(TRIM(CAST(boroid AS STRING))) IN ('2', 'BRONX', 'THE BRONX') THEN 'Bronx'
            WHEN UPPER(TRIM(CAST(boroid AS STRING))) IN ('3', 'BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
            WHEN UPPER(TRIM(CAST(boroid AS STRING))) IN ('4', 'QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
            WHEN UPPER(TRIM(CAST(boroid AS STRING))) IN ('5', 'STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
            ELSE 'UNKNOWN or CITYWIDE'
        END AS borough,

        UPPER(TRIM(CAST(streetname AS STRING))) AS street_name,
        NULLIF(TRIM(CONCAT(
            COALESCE(CAST(housenumber AS STRING), ''), ' ',
            COALESCE(CAST(streetname AS STRING), '')
        )), '') AS incident_address,  -- same name as 311's address column
        SAFE_CAST(latitude AS FLOAT64) AS latitude,
        SAFE_CAST(longitude AS FLOAT64) AS longitude,

        -- Geography extras (names assume a snake_case load of the CSV headers)
        CAST(community_district AS STRING) AS community_district,
        CAST(council_district AS STRING) AS council_district,
        CAST(census_tract AS STRING) AS census_tract,
        CAST(nta AS STRING) AS nta,

        -- Metadata
        CURRENT_TIMESTAMP() AS _stg_loaded_at

    FROM source

    -- Filters (kept light so we don't drop too many rows)
    WHERE litigationid IS NOT NULL
      AND caseopendate IS NOT NULL
      AND boroid IS NOT NULL
),

deduped AS (
    SELECT *
    FROM cleaned
    WHERE created_date IS NOT NULL
      AND CAST(created_date AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 YEAR)

    -- Deduplicate: keep one record per litigation
    -- (the source has no status-date column, so the latest open date breaks ties)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY litigation_id
        ORDER BY created_date DESC
    ) = 1
)

SELECT * FROM deduped
-- Materializes as: stg_nyc_housing_litigations
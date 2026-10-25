-- Clean and standardize DOB NOW: Build job application filings
-- One row per job filing (job_filing_number)
-- Conformed column names shared with the 311 staging model:
--   created_date, status, zip, borough, street_name, incident_address, latitude, longitude

WITH source AS (
    SELECT * FROM {{ source('raw', 'source_nyc_dob_job_apps') }}
), -- Easier to refer to the dbt reference to a long name table this way

cleaned AS (
    SELECT
        -- Get all columns from source, except ones we're transforming below.
        -- Anything not listed (job_description, building_type, stories, dwelling units,
        -- applicant_*, owner_type, and any bin/block/lot/community board columns
        -- further down the table) passes through unchanged.
        * EXCEPT (
            job_filing_number,
            filing_date,
            current_status_date,
            filing_status,
            job_type,
            postcode,
            zip,
            borough,
            house_no,
            street_name,
            bin,
            block,
            bbl,
            commmunity_board,
            initial_cost,
            total_construction_floor_area,
            latitude,
            longitude
        ),

        -- Identifiers
        CAST(job_filing_number AS STRING) AS job_filing_id,
        CAST(bin AS STRING) AS bin,
        CAST(block AS STRING) AS block,
        CAST(bbl AS STRING) AS bbl,  -- borough-block-lot key, handy for joining to PLUTO
        CAST(commmunity_board AS STRING) AS community_board,  -- fixes triple-m typo in raw column

        -- Date/Time (created_date conforms with 311)
        CAST(filing_date AS TIMESTAMP) AS created_date,
        CAST(current_status_date AS TIMESTAMP) AS status_date,

        -- Filing details
        UPPER(TRIM(CAST(filing_status AS STRING))) AS status,
        UPPER(TRIM(CAST(job_type AS STRING))) AS job_type,
        CAST(initial_cost AS FLOAT64) AS estimated_cost_usd,
        CAST(total_construction_floor_area AS FLOAT64) AS construction_floor_area_sqft,

        -- Location - clean zip code (raw column is called postcode)
        -- Raw table has both postcode and zip; prefer postcode, fall back to zip
        CASE
            WHEN UPPER(TRIM(COALESCE(NULLIF(CAST(postcode AS STRING), ''), CAST(zip AS STRING))))
                IN ('N/A', 'NA', 'NONE', '') THEN NULL
            WHEN REGEXP_CONTAINS(
                TRIM(COALESCE(NULLIF(CAST(postcode AS STRING), ''), CAST(zip AS STRING))),
                r'^\d{5}(-?\d{4})?$')
                THEN LEFT(TRIM(COALESCE(NULLIF(CAST(postcode AS STRING), ''), CAST(zip AS STRING))), 5)  -- keep 5 digits to match 311
            ELSE NULL
        END AS zip,

        -- Location - standardized borough to match 311 values
        CASE
            WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
            WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
            WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
            WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
            WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
            ELSE 'UNKNOWN or CITYWIDE'
        END AS borough,

        UPPER(TRIM(CAST(street_name AS STRING))) AS street_name,
        NULLIF(TRIM(CONCAT(
            COALESCE(CAST(house_no AS STRING), ''), ' ',
            COALESCE(CAST(street_name AS STRING), '')
        )), '') AS incident_address,  -- same name as 311's address column
        CAST(latitude AS FLOAT64) AS latitude,
        CAST(longitude AS FLOAT64) AS longitude,

        -- Metadata
        CURRENT_TIMESTAMP() AS _stg_loaded_at

    FROM source

    -- Filters (kept light so we don't drop too many rows)
    WHERE job_filing_number IS NOT NULL
      AND filing_date IS NOT NULL
      AND CAST(filing_date AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 YEAR)
      AND borough IS NOT NULL

    -- Deduplicate: keep the most recent record per job filing
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY job_filing_number
        ORDER BY current_status_date DESC, filing_date DESC
    ) = 1
)

SELECT * FROM cleaned
-- Materializes as: stg_nyc_dob_job_filings
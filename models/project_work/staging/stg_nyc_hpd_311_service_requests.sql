-- Clean and standardize HPD 311 service requests
-- One row per service request (service_request_id, from unique_key)
-- Conformed column names shared with the other staging models:
--   created_date, status, zip, borough, street_name, incident_address, latitude, longitude

WITH source AS (
    SELECT * FROM {{ source('raw', 'source_hpd_311_service_requests') }}
), -- Easier to refer to the dbt reference to a long name table this way

cleaned AS (
    SELECT
        -- Get all columns from source, except ones we're transforming below.
        -- Anything not listed (agency, agency_name, descriptor, descriptor_2, address_type,
        -- location_type, city, open_data_channel_type, park_*, resolution_description,
        -- cross_street_*, intersection_street_*, landmark) passes through unchanged.
        * EXCEPT (
            unique_key,
            created_date,
            closed_date,
            status,
            complaint_type,
            borough,
            incident_zip,
            incident_address,
            street_name,
            bbl,
            community_board,
            council_district,
            police_precinct,
            resolution_action_updated_date,
            x_coordinate_state_plane,
            y_coordinate_state_plane,
            latitude,
            longitude
        ),

        -- Identifiers
        CAST(unique_key AS STRING) AS service_request_id,
        CAST(bbl AS STRING) AS bbl,  -- borough-block-lot key, handy for joining to PLUTO
        CAST(community_board AS STRING) AS community_board,
        CAST(council_district AS STRING) AS council_district,
        CAST(police_precinct AS STRING) AS police_precinct,

        -- Date/Time (created_date is the conformed name)
        CAST(created_date AS TIMESTAMP) AS created_date,
        CAST(closed_date AS TIMESTAMP) AS closed_date,
        -- Stored as text in the raw table, so parse ISO or MM/DD/YYYY hh:mm:ss AM/PM
        COALESCE(
            SAFE_CAST(resolution_action_updated_date AS TIMESTAMP),
            SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', CAST(resolution_action_updated_date AS STRING))
        ) AS resolution_updated_date,

        -- Request details
        UPPER(TRIM(CAST(status AS STRING))) AS status,  -- conforms with the other models
        UPPER(TRIM(CAST(complaint_type AS STRING))) AS complaint_type,

        -- Location - clean zip code (raw column is called incident_zip)
        CASE
            WHEN UPPER(TRIM(CAST(incident_zip AS STRING))) IN ('N/A', 'NA', 'NONE', '') THEN NULL
            WHEN REGEXP_CONTAINS(TRIM(CAST(incident_zip AS STRING)), r'^\d{5}(-?\d{4})?$')
                THEN LEFT(TRIM(CAST(incident_zip AS STRING)), 5)  -- keep 5 digits
            ELSE NULL
        END AS zip,

        -- Location - standardized borough values (311 uses 'Unspecified' for unknowns)
        CASE
            WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
            WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
            WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
            WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
            WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
            ELSE 'UNKNOWN or CITYWIDE'
        END AS borough,

        UPPER(TRIM(CAST(street_name AS STRING))) AS street_name,
        UPPER(NULLIF(TRIM(CAST(incident_address AS STRING)), '')) AS incident_address,
        CAST(latitude AS FLOAT64) AS latitude,
        CAST(longitude AS FLOAT64) AS longitude,
        SAFE_CAST(x_coordinate_state_plane AS FLOAT64) AS x_coordinate_state_plane,
        SAFE_CAST(y_coordinate_state_plane AS FLOAT64) AS y_coordinate_state_plane,

        -- Metadata
        CURRENT_TIMESTAMP() AS _stg_loaded_at

    FROM source

    -- Filters (kept light so we don't drop too many rows)
    WHERE unique_key IS NOT NULL
      AND created_date IS NOT NULL
      AND borough IS NOT NULL
),

deduped AS (
    SELECT
        *,
        -- Hours from open to close; NULL while a request is still open
        CASE
            WHEN closed_date >= created_date
                THEN TIMESTAMP_DIFF(closed_date, created_date, HOUR)
        END AS resolution_time_hours
    FROM cleaned
    WHERE CAST(created_date AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 YEAR)

    -- Deduplicate: keep the most recently updated record per service request
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY service_request_id
        ORDER BY COALESCE(resolution_updated_date, closed_date, created_date) DESC,
                 created_date DESC
    ) = 1
)

SELECT * FROM deduped
-- Materializes as: stg_nyc_hpd_311_service_requests
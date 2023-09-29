{{ config(materialized='view') }}

SELECT DISTINCT
    application_date,
    report_id,
    report_dt AS report_date,
    applicantid,
    loan_id,
    pmtstringstart,
    -- Убираем правый "хвост", если он состоит только из 'X'
    REPLACE(REGEXP_REPLACE(pmtstring84m, 'X+$', ''), '9', '5') AS pmtstring84m,
    REVERSE(REPLACE(REGEXP_REPLACE(pmtstring84m, 'X+$', ''), '9', '5')) AS pmtstring84m_rev,
    LENGTH(REGEXP_REPLACE(pmtstring84m, 'X+$', '')) AS len,
    0 AS l_val,
    0 AS worst_status,
    0 AS count_day
FROM {{ ref('nvg_data_gr_cre_mart') }}
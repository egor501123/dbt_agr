{{ config(materialized='view') }}

SELECT
    application_date,
    report_id,
    report_dt,
    applicantid,
    loan_id,
    dlq_end_dt,
    MAX(delinq_length) AS delinq_length,
    is_active
FROM
(
   SELECT
        application_date,
        report_id,
        report_date AS report_dt,
        applicantid,
        loan_id,
        dlq_end_dt,
        CASE
            WHEN worst_status <> 5
            THEN CASE
                    WHEN worst_status = 1.5 THEN 29
                    WHEN worst_status = 2 THEN 59
                    WHEN worst_status = 3 THEN 89
                    WHEN worst_status = 4 THEN 119
                END
            WHEN worst_status = 5
            THEN EXTRACT(DAY FROM (dlq_end_dt - INTERVAL '121 DAY') - INTERVAL '1 MONTH' * (count_day - 1))::INTEGER
        END AS delinq_length,
        CASE
        WHEN report_date BETWEEN
                (dlq_end_dt - INTERVAL '1 DAY' *
                (CASE
                WHEN worst_status = 1.5 THEN 29
                WHEN worst_status = 2 THEN 59
                WHEN worst_status = 3 THEN 89
                WHEN worst_status = 4 THEN 119
                ELSE EXTRACT(DAY FROM (dlq_end_dt - INTERVAL '121 DAY') - INTERVAL '1 MONTH' * (count_day - 1))::INTEGER
                END))
            AND ((dlq_end_dt + INTERVAL '1 MONTH') - INTERVAL '1 DAY')
          THEN 1
          ELSE 0
        END AS is_active
    FROM {{ ref('nvg_data_gr_cre_dlq_t5') }}  t
    UNION ALL
    SELECT
        ist.application_date,
        ist.report_id,
        ist.report_dt,
        ist.applicantid,
        ist.loan_id,
        COALESCE(scr.dlq_end_dt, ist.Infconfirmdate) AS dlq_end_dt,
        currentdelq AS delinq_length,
        CASE WHEN report_dt BETWEEN
                 (COALESCE(scr.dlq_end_dt, ist.Infconfirmdate) - INTERVAL '1 DAY' * ist.currentdelq)
              AND ((COALESCE(scr.dlq_end_dt, ist.Infconfirmdate) + INTERVAL '1 MONTH') - INTERVAL '1 DAY')
              THEN 1 ELSE 0 END AS is_active
    FROM {{ ref('nvg_data_gr_cre_mart') }} AS ist
    LEFT JOIN {{ ref('nvg_data_gr_cre_dlq_t5') }} AS scr
        ON ist.applicantid = scr.applicantid
        AND ist.loan_id = scr.loan_id
        AND CAST(EXTRACT(DAY FROM (ist.Infconfirmdate - scr.dlq_end_dt)) AS INTEGER) BETWEEN 1 AND 30
    WHERE currentdelq > 0
)x
GROUP BY application_date, report_id, report_dt, applicantid, loan_id, dlq_end_dt, is_active


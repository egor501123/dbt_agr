{{ config(materialized='view') }}


SELECT
    applicationuid,
    application_date,
    loan_id,
    is_bank,
    applicantid,
    -- количество любых просрочек
    COUNT(CASE WHEN delinq_cutoff_date < application_date THEN 1 END) AS count_any,
    -- количество любых просрочек за 5 лет
    COUNT(CASE WHEN (application_date - interval '5 years') <= delinq_cutoff_date AND delinq_cutoff_date < application_date THEN 1 END) AS count_any_5y,
    -- длительность текущей просрочки
    MAX(CASE WHEN is_active = 1 THEN delinq_length ELSE 0 END) AS active_delinq_length,
    -- просрочки за 5 лет
    COUNT(CASE WHEN (application_date - interval '5 years') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 1 <= delinq_length AND delinq_length < 30 THEN 1 END) AS count_1_29_5y,
    COUNT(CASE WHEN (application_date - interval '5 years') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 30 <= delinq_length AND delinq_length < 60 THEN 1 END) AS count_30_59_5y,
    COUNT(CASE WHEN (application_date - interval '5 years') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 60 <= delinq_length AND delinq_length < 90 THEN 1 END) AS count_60_89_5y,
    COUNT(CASE WHEN (application_date - interval '5 years') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 90 <= delinq_length AND delinq_length < 120 THEN 1 END) AS count_90_119_5y,
    COUNT(CASE WHEN (application_date - interval '5 years') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 120 <= delinq_length THEN 1 END) AS count_120_5y,
    -- просрочки за 3 года
    COUNT(CASE WHEN (application_date - interval '3 years') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 1 <= delinq_length AND delinq_length < 30 THEN 1 END) AS count_1_29_3y,
    COUNT(CASE WHEN (application_date - interval '3 years') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 30 <= delinq_length AND delinq_length < 60 THEN 1 END) AS count_30_59_3y,
    COUNT(CASE WHEN (application_date - interval '3 years') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 60 <= delinq_length AND delinq_length < 90 THEN 1 END) AS count_60_89_3y,
    COUNT(CASE WHEN (application_date - interval '3 years') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 90 <= delinq_length AND delinq_length < 120 THEN 1 END) AS count_90_119_3y,
    COUNT(CASE WHEN (application_date - interval '3 years') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 120 <= delinq_length THEN 1 END) AS count_120_3y,
    -- просрочки за 1 год
    COUNT(CASE WHEN (application_date - interval '1 year') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 1 <= delinq_length AND delinq_length < 30 THEN 1 END) AS count_1_29_1y,
    COUNT(CASE WHEN (application_date - interval '1 year') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 30 <= delinq_length AND delinq_length < 60 THEN 1 END) AS count_30_59_1y,
    COUNT(CASE WHEN (application_date - interval '1 year') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 60 <= delinq_length AND delinq_length < 90 THEN 1 END) AS count_60_89_1y,
    COUNT(CASE WHEN (application_date - interval '1 year') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 90 <= delinq_length AND delinq_length < 120 THEN 1 END) AS count_90_119_1y,
    COUNT(CASE WHEN (application_date - interval '1 year') <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 120 <= delinq_length THEN 1 END) AS count_120_1y
FROM
    (
        -- просрочки по внешним договорам
        SELECT
            b.applicationuid,
            a.loan_id,
            b.is_bank,
            b.application_date,
            a.applicantid,
            a.dlq_end_dt AS delinq_cutoff_date,
            a.delinq_length,
            CASE
                WHEN EXTRACT(MONTH FROM AGE(b.application_date, a.report_dt)) > 1 THEN 0
                ELSE a.is_active
            END as is_active
        FROM  {{ ref('nvg_data_gr_cre_dlq') }} a
        INNER JOIN {{ ref('nvg_data_gr_cre_mart') }} b
            ON b.applicantid = a.applicantid
            AND b.report_id = a.report_id
            AND b.loan_id = a.loan_id
            AND a.application_date = b.application_date
    ) aa
GROUP BY applicationuid, application_date, applicantid, loan_id, is_bank
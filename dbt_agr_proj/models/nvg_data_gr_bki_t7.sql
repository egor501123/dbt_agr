{{ config(materialized='view') }}

SELECT
    applicationuid,
    application_date,
    loan_id,
    is_bank,
    applicantid,
    -- 6 месяцев BANK
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= (date_start + interval '6 months') AND delinquency_start_dt < application_date AND delinq_length6m > 0 THEN 1 END) AS bank_count_first_6m_1,
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= (date_start + interval '6 months') AND delinquency_start_dt < application_date AND delinq_length6m > 30 THEN 1 END) AS bank_count_first_6m_30,
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= (date_start + interval '6 months') AND delinquency_start_dt < application_date AND delinq_length6m > 60 THEN 1 END) AS bank_count_first_6m_60,
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= (date_start + interval '6 months') AND delinquency_start_dt < application_date AND delinq_length6m > 90 THEN 1 END) AS bank_count_first_6m_90,
    -- 12 месяцев BANK
    -- Здесь будут расхождения, так как исправлена опечатка в формировании delinq_length12m
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= (date_start + interval '12 months') AND delinquency_start_dt < application_date AND delinq_length12m > 0 THEN 1 END) AS bank_count_first_12m_1,
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= (date_start + interval '12 months') AND delinquency_start_dt < application_date AND delinq_length12m > 30 THEN 1 END) AS bank_count_first_12m_30,
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= (date_start + interval '12 months') AND delinquency_start_dt < application_date AND delinq_length12m > 60 THEN 1 END) AS bank_count_first_12m_60,
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= (date_start + interval '12 months') AND delinquency_start_dt < application_date AND delinq_length12m > 90 THEN 1 END) AS bank_count_first_12m_90,
    -- 6 месяцев BKI
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= (date_start + interval '6 months') AND delinquency_start_dt < application_date AND delinq_length6m > 0 THEN 1 END) AS bki_count_first_6m_1,
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= (date_start + interval '6 months') AND delinquency_start_dt < application_date AND delinq_length6m > 30 THEN 1 END) AS bki_count_first_6m_30,
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= (date_start + interval '6 months') AND delinquency_start_dt < application_date AND delinq_length6m > 60 THEN 1 END) AS bki_count_first_6m_60,
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= (date_start + interval '6 months') AND delinquency_start_dt < application_date AND delinq_length6m > 90 THEN 1 END) AS bki_count_first_6m_90,
    -- 12 месяцев BKI
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= (date_start + interval '12 months') AND delinquency_start_dt < application_date AND delinq_length12m > 0 THEN 1 END) AS bki_count_first_12m_1,
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= (date_start + interval '12 months') AND delinquency_start_dt < application_date AND delinq_length12m > 30 THEN 1 END) AS bki_count_first_12m_30,
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= (date_start + interval '12 months') AND delinquency_start_dt < application_date AND delinq_length12m > 60 THEN 1 END) AS bki_count_first_12m_60,
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= (date_start + interval '12 months') AND delinquency_start_dt < application_date AND delinq_length12m > 90 THEN 1 END) AS bki_count_first_12m_90,
    -- Флаг выхода на просрочку с первого платежа
    MAX(first_payment_default) AS first_payment_default,
    -- Флаг выхода на просрочку со второго платежа
    MAX(second_payment_default) AS second_payment_default
FROM {{ ref('nvg_data_gr_bki_t6') }}
GROUP BY
    applicationuid,
    application_date,
    loan_id,
    is_bank,
    applicantid
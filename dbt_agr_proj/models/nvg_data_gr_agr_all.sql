{{ config(materialized='view') }}

SELECT
    a.applicationuid,
    a.application_date,
    a.applicantid,
    a.report_dt,
    a.loan_id,
    a.class_type,
    a.is_bank,
    a.date_start, -- Дата открытия договора
    a.date_end_plan, -- Дата финального платежа (плановая)
    CASE
        WHEN a.date_end_fact IS NULL AND EXTRACT(DAY FROM age(application_date, a.report_dt)) > 1 AND application_date > a.date_end_plan THEN a.date_end_plan
        ELSE a.date_end_fact
    END AS date_end_fact, -- Дата закрытия счета (фактическая)
    a.credit_amount,
    a.credit_amount AS card_limit,
    CASE
        WHEN A.STATUS = '14' THEN 0
        WHEN a.product = 'КК' AND EXTRACT(DAY FROM age(application_date, a.report_dt)) > 1 THEN 0
        WHEN EXTRACT(MONTH FROM age(application_date, a.report_dt)) > 1 THEN GREATEST(a.current_debt_rur - COALESCE(A.MONTH_PMT, 0) * EXTRACT(MONTH FROM age(application_date, a.report_dt)), 0)
        ELSE a.current_debt_rur
    END AS current_debt_rur,
    COALESCE(A.MONTH_PMT, 0) AS next_pmt,
    CASE
        WHEN A.STATUS = '14' THEN 0
        WHEN a.product = 'КК' AND EXTRACT(DAY FROM age(application_date, a.report_dt)) > 1 THEN 0
        ELSE a.delinquent_debt_rur
    END AS delinquent_debt_rur,
    a.product,
    0 AS is_diff,
    0 AS is_individual_pp,
    a.currency,
    a.interest_rate_month,
    a.is_debtor,
    a.is_guarantor,
    CASE
        WHEN a.collateralcode IS NULL THEN 1 ELSE 0
    END AS coll_not_flg,
    CASE
        WHEN a.collateralcode IN ('11', '12') THEN 1 ELSE 0
    END AS coll_real_flg,
    CASE
        WHEN a.collateralcode IN ('1', '01') THEN 1 ELSE 0
    END AS coll_auto_flg
FROM {{ ref('nvg_data_gr_cre_mart') }} a
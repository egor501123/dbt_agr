{{ config(materialized='view') }}

SELECT
    a.applicationuid,
    a.application_date,
    a.applicantid,
    a.report_dt,
    a.loan_id,
    a.class_type,
    a.is_bank,
    a.date_start,
    a.date_end_plan,
    a.date_end_fact,
    a.credit_amount,
    a.card_limit,
    a.current_debt_rur,
    a.next_pmt,
    a.delinquent_debt_rur,
    a.product,
    a.is_diff,
    a.is_individual_pp,
    a.currency,
    a.interest_rate_month,
    a.is_debtor,
    a.is_guarantor,
    a.credit_amount_rur,
    a.card_limit_rur,
    a.credit_amount_rur_0,
    a.card_limit_rur_0,
    a.credit_amount_rur_b,
    a.card_limit_rur_b,
    a.remain_term,
    a.is_ik,
    a.is_pk,
    a.is_ak,
    a.is_kk,
    a.is_mk,
    a.is_ok,
    a.is_advanced,
    a.is_advanced_25,
    a.prev_date_end_fact__bank,
    a.prev_date_end_fact,
    a.is_open,
    a.for_length__date_start__bank,
    a.for_length__date_start,
    a.for_length__date_end_fact,
    a.coll_not_flg,
    a.coll_real_flg,
    a.coll_auto_flg,

    -- обязательство рассчитывается только по заёмщикам/созаёмщикам и поручителям; в остальных случаях - 0
    CASE
        -- закрытые
        WHEN a.is_open = 0 THEN 0
        -- карты
        WHEN a.is_kk = 1 THEN 0.05 * a.card_limit_rur
        -- дифференцированные
        WHEN a.remain_term <= 0 THEN 0 -- деление на 0
        WHEN a.is_diff = 1 THEN ((COALESCE(a.current_debt_rur, 0)  * (1 / a.remain_term + a.interest_rate_month))+ COALESCE(a.delinquent_debt_rur, 0))
        -- прочие (аннуитетные)
        WHEN POWER(1 + a.interest_rate_month, a.remain_term) = 0 THEN 0 -- деление на 0
        WHEN 1 - 1 / POWER(1 + a.interest_rate_month, a.remain_term) = 0 THEN 0 -- деление на 0
        ELSE ((COALESCE(a.current_debt_rur, 0)  * a.interest_rate_month / (1 - 1 / POWER(1 + a.interest_rate_month, a.remain_term) )) + COALESCE(a.delinquent_debt_rur, 0))
    END *
    -- если поручительство, то коэффициент
    CASE WHEN a.class_type = 'G' THEN 0.2 ELSE 1 END AS liability_rur,
    CASE
        -- закрытые
        WHEN a.is_open = 0 THEN 0
        -- БКИ (по алгоритму РКК)
        WHEN a.is_bank = 0 THEN a.next_pmt
        -- карты
        WHEN a.is_kk = 1 THEN 0.05 * a.card_limit_rur
        -- дифференцированные
        WHEN a.remain_term <= 0 THEN 0 -- деление на 0
        WHEN a.is_diff = 1 THEN ((COALESCE(a.current_debt_rur, 0)  * (1 / a.remain_term + a.interest_rate_month))+ COALESCE(a.delinquent_debt_rur, 0))
        -- прочие (аннуитетные)
        WHEN POWER(1 + a.interest_rate_month, a.remain_term) = 0 THEN 0 -- деление на 0
        WHEN 1 - 1 / POWER(1 + a.interest_rate_month, a.remain_term) = 0 THEN 0 -- деление на 0
        ELSE ((COALESCE(a.current_debt_rur, 0)  * a.interest_rate_month / (1 - 1 / POWER(1 + a.interest_rate_month, a.remain_term) )) + COALESCE(a.delinquent_debt_rur, 0))
    END *
    -- если поручительство, то коэффициент
    CASE WHEN a.class_type = 'G' THEN 0.2 ELSE 1 END AS liability_rur_rkk,
    -- Для определения не карт, среди открытых/закрытых, по дате открытия
    row_number() OVER (PARTITION BY a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_open, a.is_kk, a.is_bank ORDER BY a.date_start DESC, ABS(loan_id) DESC) AS start_desc_num,
    row_number() OVER (PARTITION BY a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_open, a.is_kk, a.is_bank ORDER BY a.date_start ASC, ABS(loan_id) ASC) AS start_asc_num,
    -- Для определения не карт, среди открытых/закрытых, по дате закрытия
    row_number() OVER (PARTITION BY a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_open, a.is_kk, a.is_bank ORDER BY a.date_end_fact DESC, ABS(loan_id) DESC) AS end_desc_num,
    -- Для определения ПК среди всех (открытых, закрытых)
    row_number() OVER (PARTITION BY a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_pk, a.is_bank ORDER BY a.date_start DESC, ABS(loan_id) DESC) AS start_pk_desc_num,
    row_number() OVER (PARTITION BY a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_pk, a.is_bank ORDER BY a.date_start ASC, ABS(loan_id) ASC) AS start_pk_asc_num
FROM {{ ref('nvg_data_gr_bki_t2') }} a
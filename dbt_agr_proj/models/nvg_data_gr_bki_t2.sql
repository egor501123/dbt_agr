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
    a.coll_not_flg,
    a.coll_real_flg,
    a.coll_auto_flg,
    CASE
        WHEN
            a.date_end_fact IS NOT NULL OR
            a.is_kk = 0 AND COALESCE(a.current_debt_rur, 0) + COALESCE(a.delinquent_debt_rur, 0) = 0 THEN 0
        ELSE 1
    END AS is_open,
    -- для расчёта длины кредитной истории:
    -- корректируем дату договора так, чтобы она начиналась не раньше фактической даты закрытия предыдущих договоров,
    -- чтобы исключить пересечения; если предыдущего договора нет, то оставляем дату как есть
    GREATEST(a.date_start, COALESCE((a.prev_date_end_fact__bank + interval '1 day'), a.date_start)) AS for_length__date_start__bank,
    GREATEST(a.date_start, COALESCE((a.prev_date_end_fact + interval '1 day'), a.date_start)) AS for_length__date_start,
    -- для расчёта длины кредитной истории:
    -- если договор открыт, то нужно использовать дату оценки в качестве даты фактического закрытия договора
    COALESCE(a.date_end_fact, a.application_date) AS for_length__date_end_fact
FROM {{ ref('nvg_data_gr_bki_t1') }} a
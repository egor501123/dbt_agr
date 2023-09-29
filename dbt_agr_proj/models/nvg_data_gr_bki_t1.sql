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
    -- по курсу на текущую дату
    a.credit_amount AS credit_amount_rur,
    a.card_limit AS card_limit_rur,
    -- по курсу на дату договора
    a.credit_amount AS credit_amount_rur_0,
    a.card_limit AS card_limit_rur_0,
    -- по курсу на дату последних доступных остатков
    a.credit_amount AS credit_amount_rur_b,
    a.card_limit AS card_limit_rur_b,
    -- оставшийся срок в месяцах
    EXTRACT(DAY FROM a.date_end_plan - a.application_date) / (365.25 / 12) AS remain_term,
    -- продукт
    CASE WHEN a.product = 'ИК' THEN 1 ELSE 0 END AS is_ik, -- ипотека
    CASE WHEN a.product = 'ПК' THEN 1 ELSE 0 END AS is_pk, -- потреб
    CASE WHEN a.product = 'АК' THEN 1 ELSE 0 END AS is_ak, -- авто
    CASE WHEN a.product = 'КК' THEN 1 ELSE 0 END AS is_kk, -- карты
    CASE WHEN a.product = 'МЗ' THEN 1 ELSE 0 END AS is_mk, -- микрозаймы
    CASE WHEN a.product = 'Прочее' THEN 1 ELSE 0 END AS is_ok, -- другие
    a.coll_not_flg,
    a.coll_real_flg,
    a.coll_auto_flg,
    -- досрочное погашение
    CASE WHEN a.date_end_plan <> a.date_start AND EXTRACT(DAY FROM a.date_end_fact - a.date_start)/EXTRACT(DAY FROM a.date_end_plan - a.date_start) <= 0.9 THEN 1 ELSE 0 END AS is_advanced,
    -- досрочное погашение на 25%
    CASE WHEN a.date_end_plan <> a.date_start AND EXTRACT(DAY FROM a.date_end_fact - a.date_start)/EXTRACT(DAY FROM a.date_end_plan - a.date_start) <= 0.75 THEN 1 ELSE 0 END AS is_advanced_25,
    -- для расчёта длины кредитной истории:
    -- сортируем все договоры клиента по дате договора,
    -- берём из предыдущих договоров максимальную дату фактического закрытия,
    -- открытые кредиты ограничиваются датой оценки (заявки);
    -- эта дата необходима для того, чтобы убрать периоды пересечения из последующих договоров
    MAX(COALESCE(a.date_end_fact, a.application_date)) OVER (PARTITION BY a.applicationuid, a.application_date, a.applicantid, a.class_type, a.is_bank ORDER BY a.date_start, date_end_fact ASC NULLS LAST, loan_id DESC  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_date_end_fact__bank,
    MAX(COALESCE(a.date_end_fact, a.application_date)) OVER (PARTITION BY a.applicationuid, a.application_date, a.applicantid, a.class_type ORDER BY a.date_start, date_end_fact ASC NULLS LAST, loan_id DESC  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_date_end_fact
FROM {{ ref('nvg_data_gr_agr_all') }} a
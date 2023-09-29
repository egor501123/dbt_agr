{{ config(materialized='table') }}

SELECT
	a.applicationuid,
	a.application_date,
	a.applicantid,
	------------------------------------------------------------------------
	-- количество открытых
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 THEN 1 END) AS cnt_opened, -- Количество открытых договоров, где обязательство имеет статус "открыто" (заёмщик)
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.is_ik = 1 THEN 1 END) AS mrtg_open, -- Количество открытых ипотечных кредитов (заёмщик)
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.is_pk = 1 THEN 1 END) AS cl_open, -- Количество открытых потребительских кредитов (заёмщик)
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.is_ak = 1 THEN 1 END) AS auto_open, --  Количество открытых автокредитов (заёмщик)
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.is_kk = 1 THEN 1 END) AS card_open, --  Количество открытых кредитных карт (заёмщик)
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.is_mk = 1 THEN 1 END) AS micro_open, --  Количество открытых микрозаймов (заёмщик)
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.is_ok = 1 THEN 1 END) AS other_open, --  Количество других открытых кредитов (не авто, не ипотека, не карта, не потребительский кредит) (заёмщик)
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.application_date >= (a.date_start - interval '6 months') THEN 1 END) AS cnt_opened_6m, -- Определяется количество договоров, открытых не более полугода назад (заёмщик)
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.application_date >= (a.date_start - interval '1 year') THEN 1 END) AS cnt_opened_1y,
	-------------------------------------------------------------------------------
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 THEN 1 END) AS cnt_closed,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_ik = 1 THEN 1 END) AS mrtg_closed,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 THEN 1 END) AS cl_closed,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_ak = 1 THEN 1 END) AS auto_closed,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 1 THEN 1 END) AS card_closed,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_mk = 1 THEN 1 END) AS micro_closed,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_ok = 1 THEN 1 END) AS other_closed,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) AS cl_closed_gr500k,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) AS cl_closed_gr100k,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 50000  THEN 1 END) AS cl_closed_gr50k,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 < 50000   THEN 1 END) AS cl_closed_ls50k,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.application_date >= (a.date_end_fact - interval '6 months') THEN 1 END) AS cnt_agr_closed_last6m,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.application_date >= (a.date_end_fact - interval '1 year') THEN 1 END) AS cnt_agr_closed_last1y,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.application_date >= (a.date_end_fact - interval '3 years') THEN 1 END) AS cnt_agr_closed_last3y,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND COALESCE(delinq.count_any, 0) = 0 THEN 1 END) AS cnt_closed_no_del,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND COALESCE(delinq.count_any, 0) = 0 THEN 1 END) AS cl_closed_no_del,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_ik = 1 AND COALESCE(delinq.count_any, 0) = 0 THEN 1 END) AS mrtg_closed_no_del,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_ok = 1 AND COALESCE(delinq.count_any, 0) = 0 THEN 1 END) AS other_closed_no_del,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND COALESCE(delinq.count_any, 0) = 0 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) AS cnt_closed_gr500k,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND COALESCE(delinq.count_any, 0) = 0 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) AS cnt_closed_gr100k,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND COALESCE(delinq.count_any, 0) = 0 AND a.credit_amount_rur_0 >= 50000  THEN 1 END) AS cnt_closed_gr50k,
		------------------------------------------------------------------------
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_ik = 1 THEN 1 END) AS mrtg_adv_closed,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_pk = 1 THEN 1 END) AS cl_adv_closed,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_ok = 1 THEN 1 END) AS other_adv_closed,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 THEN 1 END) AS cnt_adv_repayment,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) AS cnt_adv_repayment_gr_500k,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) AS cnt_adv_repayment_gr_100k,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 50000  THEN 1 END) AS cnt_adv_repayment_gr_50k,
	COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 < 50000   THEN 1 END) AS cnt_adv_repayment_ls_50k,
	    CASE
        WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 THEN 1 END) > 0
        THEN CAST(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 THEN 1 END)
            AS DECIMAL(38,16)) / COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 THEN 1 END)
    END AS ratio_adv_repayment, --Доля дострочно закрытых кредитов среди всех закрытых договоров

    CASE
        WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) > 0
        THEN CAST(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 500000 THEN 1 END)
            AS DECIMAL(38,16)) / COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 500000 THEN 1 END)
    END AS ratio_adv_repayment_adv_gr_500k, --Доля договоров, погашенных раньше даты планового завершения на 10 или более % с суммой выдачи более 500 000 рублей среди всех закрытых договоров с суммой выдачи более 500 000 рублей

    CASE
        WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) > 0
        THEN CAST(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 100000 THEN 1 END)
            AS DECIMAL(38,16)) / COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 100000 THEN 1 END)
    END AS ratio_adv_repayment_adv_gr_100k, --Доля договоров, погашенных раньше даты планового завершения на 10 или более % с суммой выдачи более 100 000 рублей среди всех закрытых договоров с суммой выдачи более 100 000 рублей

    CASE
        WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 50000 THEN 1 END) > 0
        THEN CAST(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 50000 THEN 1 END)
            AS DECIMAL(38,16)) / COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 50000 THEN 1 END)
    END AS ratio_adv_repayment_adv_gr_50k, --Доля договоров, погашенных раньше даты планового завершения на 10 или более % с суммой выдачи более 50 000 рублей среди всех закрытых договоров с суммой выдачи более 50 000 рублей

    CASE
        WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 < 50000 THEN 1 END) > 0
        THEN CAST(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 < 50000 THEN 1 END)
            AS DECIMAL(38,16)) / COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 < 50000 THEN 1 END)
    END AS ratio_adv_repayment_adv_ls_50k, --Доля договоров, погашенных раньше даты планового завершения на 10 или более % с суммой выдачи не более 50 000 рублей среди всех закрытых договоров с суммой выдачи не более 50 000 рублей

    CASE
        WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 THEN 1 END) > 0
        THEN CAST(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_pk = 1 THEN 1 END)
            AS DECIMAL(38,16)) / COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 THEN 1 END)
    END AS ratio_cl_closed, --Доля досрочно закрытых потребительских кредитов среди всех закрытых потребительских кредитов

    CASE
        WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) > 0
        THEN CAST(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 500000 THEN 1 END)
            AS DECIMAL(38,16)) / COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 500000 THEN 1 END)
    END AS ratio_cl_closed_gr500k, --Доля потребительских кредитов, погашенных раньше даты планового завершения на 10 или более %, с суммой обязательств более 500 000 рублей среди всех закрытых потребительских кредитов с суммой обязательств более 500 000 рублей

    CASE
        WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) > 0
        THEN CAST(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 100000 THEN 1 END)
            AS DECIMAL(38,16)) / COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 100000 THEN 1 END)
    END AS ratio_cl_closed_gr100k, --Доля потребительских кредитов, погашенных раньше даты планового завершения на 10 или более %, с суммой обязательств более 100 000 рублей среди всех закрытых потребительских кредитов с суммой обязательств более 100 000 рублей

    CASE
        WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 50000 THEN 1 END) > 0
        THEN CAST(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 50000 THEN 1 END)
            AS DECIMAL(38,16)) / COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 50000 THEN 1 END)
    END AS ratio_cl_closed_gr50k,

    CAST(CASE
		WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 < 50000 THEN 1 END) > 0
		THEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_pk = 1 AND a.credit_amount_rur_0 < 50000 THEN 1 END) /
			COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 < 50000 THEN 1 END)
	END as decimal(38,16)) AS ratio_cl_closed_ls50,

    CAST(COALESCE(AVG(CASE WHEN a.is_debtor = 1 AND a.is_kk = 0 AND a.is_advanced = 1 THEN a.credit_amount_rur_0 END), 0)
        AS DECIMAL(38,16)) AS avg_liab_sum_total_adv_agr, --Средняя переоценённая в рубли сумма обязательства по договорам, закрытым раньше даты планового погашения на 10 или более %

    CAST(AVG(CASE WHEN a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 THEN EXTRACT(MONTH FROM AGE(a.date_end_fact, a.date_end_plan)) END)
        AS DECIMAL(38,16)) AS avg_diff_plan_fact_closed, --Среднее значение количества месяцев между датами планового и фактического погашения по всем закрытым договорам

    CAST(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_advanced = 1 THEN a.credit_amount_rur_0 ELSE 0 END)
        AS DECIMAL(28,6)) AS total_liab_sum_bank_adv_agr, --Общая переоценённая в рубли сумма обязательства по кредитам, закрытым досрочно на 10 или более %, по данным Банка

    CAST(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_advanced = 1 THEN a.credit_amount_rur_0 ELSE 0 END)
        AS DECIMAL(28,6)) AS total_liab_sum_bki_adv_agr, --Общая переоценённая в рубли сумма обязательства по кредитам, закрытым досрочно на 10 или более %, по данным БКИ

    CAST(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced_25 = 1 AND a.is_kk = 0 THEN 1 END)
        AS INT) AS cnt_adv25_closed, --Количество договоров погашенных раньше даты планового завершения на 25 или более %

    CAST(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_advanced_25 = 1 THEN a.credit_amount_rur_0 ELSE 0 END)
        AS DECIMAL(28,6)) AS total_liab_sum_bank_adv25_agr, --Общая переоценённая в рубли сумма обязательства по кредитам, закрытым досрочно на 25 или более %, по данным Банка

    CAST(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_advanced_25 = 1 THEN a.credit_amount_rur_0 ELSE 0 END)
        AS DECIMAL(28,6)) AS total_liab_sum_bki_adv25_agr,

    cast(COUNT(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND delinq.active_delinq_length > 0   AND a.delinquent_debt_rur >= 500              THEN 1 END) as int) AS curdel_0plus_bank, -- Рассчитывается только для договоров, открытых в Банке, без учёта договоров поручительства. Количество договоров с текущей просрочкой более 500 руб. и длительностью просрочки более 0 дней.
	cast(COUNT(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND delinq.active_delinq_length > 0   AND a.delinquent_debt_rur >= 500              THEN 1 END) as int) AS curdel_0plus_bki, -- Рассчитывается только для договоров, открытых в в других банках, без учёта договоров поручительства. Количество договоров с текущей просрочкой более 500 руб. и длительностью просрочки более 0 дней.
	cast(COUNT(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND delinq.active_delinq_length >= 30 AND a.delinquent_debt_rur >= 500              THEN 1 END) as int) AS curdel_30plus_bank, -- Рассчитывается только для договоров, открытых в Банке без учёта договоров поручительства. Количество договоров с текущей просрочкой более 500 руб. и длительностью просрочки более 30 дней.
	cast(COUNT(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND delinq.active_delinq_length >= 30 AND a.delinquent_debt_rur >= 500              THEN 1 END) as int) AS curdel_30plus_bki, -- Рассчитывается только для договоров, открытых в в других банках, без учёта договоров поручительства. Количество договоров с текущей просрочкой более 500 руб. и длительностью просрочки более 30 дней.
	cast(COUNT(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND delinq.active_delinq_length >= 60 AND a.delinquent_debt_rur >= 500              THEN 1 END) as int) AS curdel_60plus_bank, -- Рассчитывается только для договоров, открытых в Банке без учёта договоров поручительства. Количество договоров с текущей просрочкой более 500 руб. и длительностью просрочки более 60 дней.
	cast(COUNT(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND delinq.active_delinq_length >= 60 AND a.delinquent_debt_rur >= 500              THEN 1 END) as int) AS curdel_60plus_bki, -- Рассчитывается только для договоров, открытых в в других банках, без учёта договоров поручительства. Количество договоров с текущей просрочкой более 500 руб. и длительностью просрочки более 60 дней.
	cast(COUNT(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND delinq.active_delinq_length > 0   AND 0 < a.delinquent_debt_rur AND a.delinquent_debt_rur < 500 THEN 1 END) as int) AS curdel_tech_bank, -- Рассчитывается только для договоров, открытых в Банке без учёта договоров поручительства. Определяется количество договоров с текущей просрочкой >0 и <500 руб. и длительностью просрочки более 0 дней.
	cast(COUNT(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND delinq.active_delinq_length > 0   AND 0 < a.delinquent_debt_rur AND a.delinquent_debt_rur < 500 THEN 1 END) as int) AS curdel_tech_bki,


	CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_1_29_5y END), 0) AS INT) AS bank_1_29_5y_debtor, -- Общее число просрочек до 30 дней в Банке
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_30_59_5y END), 0) AS INT) AS bank_30_59_5y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_60_89_5y END), 0) AS INT) AS bank_60_89_5y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_90_119_5y END), 0) AS INT) AS bank_90_119_5y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_120_5y END), 0) AS INT) AS bank_120plus_5y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_1_29_3y END), 0) AS INT) AS bank_1_29_3y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_30_59_3y END), 0) AS INT) AS bank_30_59_3y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_60_89_3y END), 0) AS INT) AS bank_60_89_3y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_90_119_3y END), 0) AS INT) AS bank_90_119_3y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_120_3y END), 0) AS INT) AS bank_120plus_3y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_1_29_1y END), 0) AS INT) AS bank_1_29_1y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_30_59_1y END), 0) AS INT) AS bank_30_59_1y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_60_89_1y END), 0) AS INT) AS bank_60_89_1y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_90_119_1y END), 0) AS INT) AS bank_90_119_1y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_120_1y END), 0) AS INT) AS bank_120plus_1y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_1_29_5y END), 0) AS INT) AS bki_1_29_5y_debtor, -- Общее число просрочек до 30 дней по данным БКИ
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_30_59_5y END), 0) AS INT) AS bki_30_59_5y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_60_89_5y END), 0) AS INT) AS bki_60_89_5y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_90_119_5y END), 0) AS INT) AS bki_90_119_5y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_120_5y END), 0) AS INT) AS bki_120plus_5y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_1_29_3y END), 0) AS INT) AS bki_1_29_3y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_30_59_3y END), 0) AS INT) AS bki_30_59_3y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_60_89_3y END), 0) AS INT) AS bki_60_89_3y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_90_119_3y END), 0) AS INT) AS bki_90_119_3y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_120_3y END), 0) AS INT) AS bki_120plus_3y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_1_29_1y END), 0) AS INT) AS bki_1_29_1y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_30_59_1y END), 0) AS INT) AS bki_30_59_1y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_60_89_1y END), 0) AS INT) AS bki_60_89_1y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_90_119_1y END), 0) AS INT) AS bki_90_119_1y_debtor,
    CAST(COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_120_1y END), 0) AS INT) AS bki_120plus_1y_debtor,

    cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN (DATE_PART('day', a.application_date + interval '1 day' - a.date_start) / 30.44) ELSE 0 END) as decimal(38,16)) AS length_bank,
cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN (DATE_PART('day', a.application_date + interval '1 day' - a.date_start) / 30.44) ELSE 0 END) as decimal(38,16)) AS length_bki,
cast(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.for_length__date_end_fact >= for_length__date_start__bank THEN (DATE_PART('day', for_length__date_end_fact + interval '1 day' - for_length__date_start__bank) / 30.44) ELSE 0 END) / (365.25/ 12) as decimal(38,16)) AS length_bank_total,
cast(SUM(CASE WHEN a.is_debtor = 1 AND a.for_length__date_end_fact >= for_length__date_start THEN (DATE_PART('day', for_length__date_end_fact + interval '1 day' - for_length__date_start) / 30.44) ELSE 0 END) / (365.25/ 12) as decimal(38,16)) AS length_total,
cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_ik = 1 AND a.is_open = 1 AND COALESCE(delinq.count_any_5y, 0) = 0 AND a.date_start < (a.application_date - interval '6 months') AND COALESCE(delinq.active_delinq_length, 0) = 0 AND COALESCE(a.delinquent_debt_rur, 0) = 0 THEN 1 ELSE 0 END) as smallint) AS good_mortgage_bank_open,
cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_ik = 1 AND a.is_open = 1 AND COALESCE(delinq.count_any_5y, 0) = 0 AND a.date_start < (a.application_date - interval '6 months') AND COALESCE(delinq.active_delinq_length, 0) = 0 AND COALESCE(a.delinquent_debt_rur, 0) = 0 THEN 1 ELSE 0 END) as smallint) AS good_mortgage_bki_open,
cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_ik = 1 AND a.is_open = 0 AND COALESCE(delinq.count_any, 0) = 0 THEN 1 ELSE 0 END) as smallint) AS good_mortgage_bank_closed,
cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_ik = 1 AND a.is_open = 0 AND COALESCE(delinq.count_any, 0) = 0 THEN 1 ELSE 0 END) as smallint) AS good_mortgage_bki_closed,

cast(COALESCE(AVG(CASE WHEN a.is_debtor = 1 AND a.is_kk = 0 THEN a.credit_amount_rur_0 END), 0) as decimal(38,16)) AS avg_liab_sum_total,
cast(COALESCE(AVG(CASE WHEN a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 THEN a.credit_amount_rur_0 END), 0) as decimal(38,16)) AS avg_liab_sum_total_open_agr,
cast(COALESCE(AVG(CASE WHEN a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 THEN a.credit_amount_rur_0 END), 0) as decimal(38,16)) AS avg_liab_sum_total_closed_agr,
cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS max_liab_sum_bank_open,
cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS max_liab_sum_bki_open,
cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS max_liab_sum_bank_closed,
cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS max_liab_sum_bki_closed,
cast(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS total_liab_sum_bank_open_agr,
cast(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS total_liab_sum_bki_open_agr,
cast(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS total_liab_sum_bank_closed_agr,
cast(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS total_liab_sum_bki_closed_agr,


cast(SUM(CASE WHEN a.is_debtor = 1 THEN COALESCE(a.current_debt_rur, 0) + COALESCE(a.delinquent_debt_rur, 0) ELSE 0 END) as decimal(38,16)) AS outstanding,
cast(SUM(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 THEN COALESCE(a.current_debt_rur, 0) + COALESCE(a.delinquent_debt_rur, 0) ELSE 0 END) as decimal(38,16)) AS outstanding_open,
cast(COALESCE(AVG(CASE WHEN a.is_debtor = 1 AND a.delinquent_debt_rur > 0 THEN a.delinquent_debt_rur END), 0) as decimal(38,16)) AS avg_delinquent_debt_total,
cast(SUM(CASE WHEN a.is_debtor = 1 THEN a.delinquent_debt_rur ELSE 0 END) as decimal(28,6)) AS curr_arrear_rur,
cast(CASE
    WHEN SUM(CASE WHEN a.is_debtor = 1 AND a.is_kk = 1 AND a.is_open = 1 THEN a.card_limit_rur_b END) > 0 THEN
        SUM(CASE WHEN a.is_debtor = 1 AND a.is_kk = 1 AND a.is_open = 1 THEN COALESCE(a.current_debt_rur, 0) + COALESCE(a.delinquent_debt_rur, 0) END) /
        SUM(CASE WHEN a.is_debtor = 1 AND a.is_kk = 1 AND a.is_open = 1 THEN a.card_limit_rur_b END)
END as decimal(38,16)) AS utilization_avg,
cast(COALESCE(MAX(a.is_guarantor), 0) +
    COALESCE(MAX(CASE WHEN a.is_debtor = 1 THEN a.is_ik END), 0) +
    COALESCE(MAX(CASE WHEN a.is_debtor = 1 THEN a.is_pk END), 0) +
    COALESCE(MAX(CASE WHEN a.is_debtor = 1 THEN a.is_ak END), 0) +
    COALESCE(MAX(CASE WHEN a.is_debtor = 1 THEN a.is_kk END), 0) +
    COALESCE(MAX(CASE WHEN a.is_debtor = 1 THEN a.is_mk END), 0) +
    COALESCE(MAX(CASE WHEN a.is_debtor = 1 THEN a.is_ok END), 0) as int) AS cnt_liability_types,
cast(COALESCE(SUM(a.liability_rur), 0) as decimal(38,16)) AS total_curr_payment,
cast(CASE WHEN COUNT(CASE WHEN a.is_guarantor = 1 THEN 1 END) = COUNT(*) THEN 1 ELSE 0 END as smallint) AS only_guarantees,
cast(MAX(cre.inquiry1week) as integer) AS cnt_inquiry_last1w,
cast(MAX(cre.inquiry1month) as integer) AS cnt_inquiry_last1m,
cast(MAX(cre.inquiry3month) as integer) AS cnt_inquiry_last3m,
cast(MAX(cre.inquiry6month) as integer) AS cnt_inquiry_last6m,
cast(MAX(a.report_dt) as text) AS CRE_DATE,
cast(COALESCE(SUM(a.liability_rur_rkk), 0) as decimal(38,16)) AS total_curr_payment_rkk,
cast(COALESCE(SUM(CASE WHEN a.is_bank = 1 THEN a.liability_rur END), 0) as decimal(38,16)) AS BANK_LIAB_PAYM,
cast(COALESCE(SUM(CASE WHEN a.is_bank = 0 THEN a.liability_rur_rkk END), 0) as decimal(38,16)) AS BKI_LIAB_PAYM,
cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 AND a.end_desc_num = 1 THEN date_part('year', a.application_date - a.date_end_fact) * 12 + date_part('month', a.application_date - a.date_end_fact) END) as decimal(38,16)) AS bki_loan_lst_close_m_term,
cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 AND a.start_desc_num = 1 THEN a.remain_term END) as decimal(38,16)) AS bki_loan_till_lst_close_m_term,
cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 AND a.end_desc_num = 1 THEN date_part('year', a.application_date - a.date_end_fact) * 12 + date_part('month', a.application_date - a.date_end_fact) END) as decimal(38,16)) AS bank_loan_lst_close_m_term,
cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 AND a.start_desc_num = 1 THEN a.remain_term END) as decimal(38,16)) AS bank_loan_till_lst_close_m_term,
cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_pk = 1 AND a.start_pk_desc_num = 1 THEN a.interest_rate_month * 100 * 12 END) as decimal(28,6)) AS bank_loan_lst_rate,
cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_pk = 1 AND a.start_pk_asc_num = 1 THEN a.interest_rate_month * 100 * 12 END) as decimal(28,6)) AS bank_loan_fst_rate,
cast(CASE WHEN COUNT(CASE WHEN a.is_bank = 1 THEN 1 END) > 0 THEN 1 ELSE 0 END as smallint) AS bank_ki_flg,
cast(CASE WHEN COUNT(CASE WHEN a.is_bank = 0 THEN 1 END) > 0 THEN 1 ELSE 0 END as smallint) AS bki_app_flg,

CAST(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_pk = 1 THEN a.liability_rur_rkk END) AS DECIMAL(38,16)) AS max_paym_bank,
CAST(MIN(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_pk = 1 THEN a.liability_rur_rkk END) AS DECIMAL(38,16)) AS min_paym_bank,
CAST(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_pk = 1 AND a.is_open = 1 THEN a.liability_rur_rkk END) AS DECIMAL(38,16)) AS max_paym_bank_act,
CAST(MIN(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_pk = 1 AND a.is_open = 1 THEN a.liability_rur_rkk END) AS DECIMAL(38,16)) AS min_paym_bank_act,
CAST(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN a.delinquent_debt_rur ELSE 0 END) AS DECIMAL(28,6)) AS current_overdue_bank_amt,

COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_6m_1  END), 0)::integer AS bank_count_first_6m_1,
    COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_6m_30  END), 0)::integer AS bank_count_first_6m_30,
    COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_6m_60  END), 0)::integer AS bank_count_first_6m_60,
    COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_6m_90  END), 0)::integer AS bank_count_first_6m_90,
    COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_12m_1  END), 0)::integer AS bank_count_first_12m_1,
    COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_12m_30  END), 0)::integer AS bank_count_first_12m_30,
    COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_12m_60  END), 0)::integer AS bank_count_first_12m_60,
    COALESCE(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_12m_90  END), 0)::integer AS bank_count_first_12m_90,
    COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_6m_1 END), 0)::integer AS bki_count_first_6m_1,
    COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_6m_30 END), 0)::integer AS bki_count_first_6m_30,
    COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_6m_60 END), 0)::integer AS bki_count_first_6m_60,
    COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_6m_90 END), 0)::integer AS bki_count_first_6m_90,
    COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_12m_1 END), 0)::integer AS bki_count_first_12m_1,
    COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_12m_30 END), 0)::integer AS bki_count_first_12m_30,
    COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_12m_60 END), 0)::integer AS bki_count_first_12m_60,
    COALESCE(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_12m_90 END), 0)::integer AS bki_count_first_12m_90,
    COALESCE(MAX(first_payment_default), 0)::smallint AS first_payment_default,
    COALESCE(MAX(CASE WHEN first_payment_default = 0 THEN second_payment_default END), 0)::smallint AS second_payment_default,
    SUBSTRING(CAST(NOW() AS VARCHAR), 1, 19) AS t_changed_dttm,
    0::smallint AS t_deleted_flg,
    1::smallint AS t_active_flg





FROM  {{ ref('nvg_data_gr_bki_t4') }} a
-- просрочки
LEFT JOIN {{ ref('nvg_data_gr_bki_t5') }} delinq
    ON delinq.application_date = a.application_date
    AND delinq.loan_id = a.loan_id
    AND delinq.is_bank = a.is_bank
    AND delinq.applicantid = a.applicantid
    AND delinq.applicationuid = a.applicationuid
--количество выходов на просрочку за 6/12 с даты выдачи кредита.
LEFT JOIN {{ ref('nvg_data_gr_bki_t7') }} del_first
    ON del_first.application_date = a.application_date
    AND del_first.loan_id = a.loan_id
    AND del_first.is_bank = a.is_bank
    AND del_first.applicantid = a.applicantid
    AND del_first.applicationuid = a.applicationuid
-- количество запросов в БКИ
LEFT JOIN
(
    -- таблица сформирована в разрезе договоров, которые привязались к заявке из CRE;
    -- поэтому схлопываем строки, т. к. интересуемые поля относятся к заявке в целом, а не к конкретному договору
    SELECT DISTINCT
        application_date,
        applicantid,
        -- пропуски заполняем нулями, чтобы отличать эти данные (которые получены из CRE)
        -- от строк, к оторым данные из CRE не привязались (там будет NULL)
        COALESCE(inquiry1week, 0) AS inquiry1week,
        COALESCE(inquiry1month, 0) AS inquiry1month,
        COALESCE(inquiry3month, 0) AS inquiry3month,
        COALESCE(inquiry6month, 0) AS inquiry6month
    FROM {{ ref('nvg_data_gr_cre_mart') }}
) cre
    ON cre.application_date = a.application_date
    AND cre.applicantid = a.applicantid
LEFT JOIN {{ ref('nvg_data_gr_bki_payment') }} AS pay
    ON pay.application_date = a.application_date
    AND pay.loan_id = a.loan_id
    AND pay.is_bank = a.is_bank
    AND pay.applicantid = a.applicantid
    AND pay.applicationuid = a.applicationuid
GROUP BY
    a.applicationuid,
    a.application_date,
    a.applicantid
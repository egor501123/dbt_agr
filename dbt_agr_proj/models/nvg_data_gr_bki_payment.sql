{{ config(materialized='view') }}


SELECT
    applicationuid,
    application_date,
    loan_id,
    is_bank,
    applicantid,
	is_ik,  --ипотека
	is_pk,  --потреб
	is_kk,
	liability_rur_rkk ,
	liab_pk_desc_num as liab_pk_desc_any_num,
	liab_ik_desc_num as liab_ik_desc_any_num,
	liab_kk_desc_num as liab_kk_desc_any_num,
    -- просрочки за 1 год
    COUNT(CASE WHEN date_trunc('year', application_date) = date_trunc('year', delinq_cutoff_date) AND 1 <= delinq_length AND delinq_length < 30 THEN 1 END) AS bank_count_1_29_1y,
    COUNT(CASE WHEN date_trunc('year', application_date) = date_trunc('year', delinq_cutoff_date) AND 30 <= delinq_length AND delinq_length < 60 THEN 1 END) AS bank_count_30_59_1y,
    COUNT(CASE WHEN date_trunc('year', application_date) = date_trunc('year', delinq_cutoff_date) AND 60 <= delinq_length AND delinq_length < 90 THEN 1 END) AS bank_count_60_89_1y,
    COUNT(CASE WHEN date_trunc('year', application_date) = date_trunc('year', delinq_cutoff_date) AND 90 <= delinq_length AND delinq_length < 120 THEN 1 END) AS bank_count_90_119_1y,
    COUNT(CASE WHEN date_trunc('year', application_date) = date_trunc('year', delinq_cutoff_date) AND 120 <= delinq_length THEN 1 END) AS bank_count_120_1y
FROM
    (
        SELECT
            b.applicationuid,
            a.loan_id,
            b.is_bank,
			t4.is_debtor,
			t4.is_open,
            b.application_date,
            a.applicantid,
			t4.is_ik,  --ипотека
			t4.is_pk,  --потреб
			t4.is_kk,  --кредитные карты
			t4.liability_rur_rkk ,
			t4.liab_pk_desc_num,
			t4.liab_ik_desc_num,
			t4.liab_kk_desc_num ,
            a.dlq_end_dt AS delinq_cutoff_date,
            a.delinq_length
        FROM  {{ ref('nvg_data_gr_cre_dlq') }} a
        INNER join  {{ ref('nvg_data_gr_cre_mart') }} b
            on b.applicantid = a.applicantid
            and b.report_id = a.report_id
            and b.loan_id = a.loan_id
			and a.application_date = b.application_date
		INNER join {{ ref('nvg_data_gr_bki_t4') }} t4
			on  b.applicantid = t4.applicantid
            and b.loan_id = t4.loan_id
			and a.application_date = t4.application_date
		WHERE t4.is_bank = 0
    ) a
GROUP BY applicationuid,
		application_date,
		applicantid,
		loan_id,
		is_bank,
		is_ik,
		is_pk,
		is_kk,
		liability_rur_rkk ,
		liab_pk_desc_num,
		liab_ik_desc_num,
		liab_kk_desc_num
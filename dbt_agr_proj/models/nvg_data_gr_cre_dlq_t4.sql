{{ config(materialized='view') }}

SELECT
	application_date,
	report_id,
	report_date,
	applicantid,
	loan_id,
	pmtstringstart,
	pmtstring84m,
	pmtstring84m_rev as st,
	pmtstring84m_rev,
	len,
	l_val,
	worst_status,
	count_day
FROM {{ ref('nvg_data_gr_cre_dlq_t2') }}
WHERE len > 1
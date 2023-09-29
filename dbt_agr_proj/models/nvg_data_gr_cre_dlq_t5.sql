{{ config(materialized='view') }}

select
	application_date,
	report_id,
	report_date,
	applicantid,
	loan_id,
	pmtstringstart as dlq_end_dt,
	pmtstring84m,
	pmtstring84m_rev,
--записываем единственный статус как худший
	case
		when substr(pmtstring84m_rev, 1, 1) = 'X' then 1
		when substr(pmtstring84m_rev, 1, 1) = 'A' then 1.5
		when substr(pmtstring84m_rev, 1, 1) in ('0', '1', '2', '3', '4', '5', '7', '8', '9') then cast(substr(pmtstring84m_rev, 1, 1) as int)
		else 1
	end as worst_status,
	case when substr(pmtstring84m_rev, 1, 1) = '5' then 1 else 0 end count_day
FROM {{ ref('nvg_data_gr_cre_dlq_t2') }}
where len = 1
{{ config(materialized='view') }}

SELECT
    b.applicationuid,
    a.loan_id,
    is_bank,
    b.application_date,
    a.applicantid,
    b.date_start,
    -- Дата выхода на просрочку
    -- Так как по БКИ приходит группа просрочки, то считая дату начала возможна ситуация, когда дата_договора > дата начала просрочки
    CASE WHEN (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) < b.date_start
        THEN b.date_start
        ELSE (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length))
    END AS delinquency_start_dt,
    -- Длительность до истечения 6 месяцев
    DATE_PART('day', LEAST(a.dlq_end_dt, b.date_start + interval '6 months') + interval '1 day' -
        CASE WHEN (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) < b.date_start
            THEN b.date_start
            ELSE (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length))
        END) AS delinq_length6m,
    -- Длительность до истечения 12 месяцев
    DATE_PART('day', LEAST(a.dlq_end_dt, b.date_start + interval '12 months') + interval '1 day' -
        CASE WHEN (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) < b.date_start
            THEN b.date_start
            ELSE (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length))
        END) AS delinq_length12m,
    -- Общая длительность просрочки
    a.delinq_length,
    CASE WHEN DATE_PART('month',
            CASE WHEN (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) < b.date_start
                THEN b.date_start
                ELSE (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length))
            END - b.date_start) < 2
            AND a.delinq_length > 90
         THEN 1
         ELSE 0
    END AS first_payment_default,
    CASE WHEN DATE_PART('month',
            CASE WHEN (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) < b.date_start
                THEN b.date_start
                ELSE (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length))
            END - b.date_start) > 2
            AND DATE_PART('month',
                CASE WHEN (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) < b.date_start
                    THEN b.date_start
                    ELSE (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length))
                END - b.date_start) < 3
            AND a.delinq_length > 90
         THEN 1
         ELSE 0
    END AS second_payment_default
FROM {{ ref('nvg_data_gr_cre_dlq') }} a
INNER JOIN {{ ref('nvg_data_gr_cre_mart') }}  b
    ON b.applicantid = a.applicantid
    AND b.report_id = a.report_id
    AND b.loan_id = a.loan_id
    AND a.application_date = b.application_date
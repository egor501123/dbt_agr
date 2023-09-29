{{ config(materialized='view') }}

WITH ranked_data AS (
    SELECT
        mt.applicationuid,
        'IKAR' AS rep_type,
        '' AS second_name,
        '' AS first_name,
        '' AS third_name,
        '' AS birth_dt,
        mt.applicantid,
        mt.applicationuid AS cre_natural_loan_request_id,
        mt.hjid AS name_id,
        mt.hjid AS report_id,
        l.hjid AS loan_id,
        TO_TIMESTAMP(SUBSTRING(mt.reportdatetime, 1, 8), 'DDMMYYYY') AS report_dt,
        CASE
            WHEN CAST(l.relationship AS INTEGER) <= 4 THEN 'D'
            WHEN CAST(l.relationship AS INTEGER) = 5 THEN 'G'
            ELSE NULL
        END AS class_type,
        l.isown AS is_bank,
        TO_TIMESTAMP(l.opendate, 'DDMMYYYY') AS date_start,
        TO_TIMESTAMP(l.finalpmtdate, 'DDMMYYYY') AS date_end_plan,
        TO_TIMESTAMP(l.factclosedate, 'DDMMYYYY') AS date_end_fact,
        l.creditlimit AS credit_amount,
        l.outstanding AS current_debt_rur,
        l.delqbalance AS delinquent_debt_rur,
        CASE
            WHEN l.type_ = '1' THEN 'АК'
            WHEN l.type_ = '6' THEN 'ИК'
            WHEN l.type_ = '7' THEN 'КК'
            WHEN l.type_ = '8' THEN 'КК'
            WHEN l.type_ = '21' THEN 'МЗ'
            WHEN l.type_ = '9' THEN 'ПК'
            ELSE 'Прочее'
        END AS product,
        l.currency,
        l.interestrate / 100 / 12 AS interest_rate_month,
        CASE
            WHEN CAST(l.relationship AS INTEGER) <= 4 THEN 1
            ELSE 0
        END AS is_debtor,
        CASE
            WHEN CAST(l.relationship AS INTEGER) = 5 THEN 1
            ELSE 0
        END AS is_guarantor,
        l.pmtstring84m,
        TO_TIMESTAMP(l.pmtstringstart, 'DDMMYYYY') AS pmtstringstart,
        TO_TIMESTAMP(l.Infconfirmdate, 'DDMMYYYY') AS infconfirmdate,
        CAST(l.currentdelq AS INTEGER) AS currentdelq,
        ov.inquiry1week AS inquiry1week,
        ov.inquiry1month AS inquiry1month,
        ov.inquiry3month AS inquiry3month,
        ov.inquiry6month AS inquiry6month,
        l.nextpmt AS next_pmt,
        COALESCE(l.businesscategory, 1) AS business_category,
        l.status,
        l.relationship,
        l.type_,
        l.uuid,
        l.creditcostrate AS credit_cost_rate,
        '01.01.06' AS programcode,
        l.principal_past_due,
        l.collateralcode AS collateralcode,
        l.principal_outstanding,
        RANK() OVER (
            PARTITION BY mt.applicantid
            ORDER BY TO_TIMESTAMP(mt.reportdatetime, 'DDMMYYYYHH24MISS') DESC,
                     mt.hjid DESC
        ) AS rn
    FROM {{ source('cre_data', 'singleformattype') }} sft
    INNER JOIN {{ source('cre_data', 'maintype') }} mt ON mt.hjid = sft.main
    INNER JOIN {{ source('cre_data', 'loanstype') }} l ON l.loanstypes_loan_hjid = sft.loans
    LEFT JOIN {{ source('cre_data', 'loansoverviewtype') }} ov ON ov.hjid = sft.loansoverview
    WHERE 1=1
	AND cast(l.relationship AS INTEGER) <= 5
	AND l.TYPE_ NOT IN (
	'17' ,
	'18' ,
	'19'
	)
    and length(coalesce(replace(regexp_replace(l.pmtstring84m,'X+$',''),'9','5'),'')) <= 2000
)

SELECT DISTINCT *
FROM  ranked_data
WHERE rn = 1
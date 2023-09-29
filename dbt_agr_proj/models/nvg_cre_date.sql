{{ config(materialized='view') }}

SELECT {{ dbt.safe_cast("'2023-03-01'", api.Column.translate_type("date")) }} AS application_date
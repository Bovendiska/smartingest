-- A cashier sometimes types the total by hand. SmartIngest checks formats, not arithmetic,
-- so this business rule lives in dbt. Warn only: the sale happened, finance should look at it.
{{ config(severity='warn') }}
select transaction_id, branch_code, quantity, unit_price, amount
from {{ ref('stg_retailku__transactions') }}
where amount <> quantity * unit_price

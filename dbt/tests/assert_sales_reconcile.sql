-- Gold must not lose or invent money: monthly revenue adds up to the staged transactions.
select g.revenue, s.revenue
from (select sum(revenue) as revenue from {{ ref('sales_by_branch_month') }}) as g
cross join (select coalesce(sum(amount), 0) as revenue from {{ ref('stg_retailku__transactions') }}) as s
where g.revenue <> s.revenue

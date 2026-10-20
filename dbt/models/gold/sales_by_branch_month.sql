-- Monthly sales per branch from validated transactions only.
-- rows_pending_review says how many transaction rows of that branch-month are still in quarantine,
-- so nobody mistakes "not reviewed yet" for "no sales".
with sales as (
    select
        branch_code,
        date_trunc('month', transaction_date)::date        as month,
        count(*)                                            as transactions,
        sum(quantity)                                       as units_sold,
        sum(amount)                                         as revenue,
        sum(amount) filter (where not is_walk_in)           as member_revenue,
        count(distinct customer_id)                         as active_members,
        count(*) filter (where payment_method = 'QRIS')     as qris_transactions
    from {{ ref('stg_retailku__transactions') }}
    group by 1, 2
),

pending as (
    select
        c.branch_code,
        date_trunc('month', c.business_date)::date as month,
        sum(b.rows_quarantined)                    as rows_pending_review
    from {{ ref('stg_batch_context') }} as c
    inner join {{ ref('stg_meta__batches') }} as b using (batch_id)
    where c.source_name = 'retailku_transactions'
    group by 1, 2
)

select
    coalesce(s.branch_code, p.branch_code)                      as branch_code,
    coalesce(s.month, p.month)                                  as month,
    coalesce(s.transactions, 0)                                 as transactions,
    coalesce(s.units_sold, 0)                                   as units_sold,
    coalesce(s.revenue, 0)                                      as revenue,
    round(s.revenue::numeric / nullif(s.transactions, 0))       as avg_transaction_value,
    round(100.0 * s.member_revenue / nullif(s.revenue, 0), 1)   as member_revenue_pct,
    coalesce(s.active_members, 0)                               as active_members,
    round(100.0 * s.qris_transactions / nullif(s.transactions, 0), 1) as qris_pct,
    coalesce(p.rows_pending_review, 0)                          as rows_pending_review
from sales as s
full outer join pending as p on s.branch_code = p.branch_code and s.month = p.month

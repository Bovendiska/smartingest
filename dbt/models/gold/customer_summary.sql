-- One row per member with purchase behaviour and a simple recency segment.
-- No names, emails or phone numbers: gold is for analysis, contact data stays in silver.
-- Recency is measured against the latest sale in the data, not today's date, so rebuilding is reproducible.
with purchases as (
    select
        customer_id,
        min(transaction_date)   as first_purchase_on,
        max(transaction_date)   as last_purchase_on,
        count(*)                as transactions,
        sum(quantity)           as units_bought,
        sum(amount)             as total_spent
    from {{ ref('stg_retailku__transactions') }}
    where customer_id is not null
    group by customer_id
),

as_of as (
    select max(transaction_date) as as_of_date from {{ ref('stg_retailku__transactions') }}
),

joined as (
    select
        c.customer_id,
        c.branch_code,
        c.city,
        c.is_member,
        c.registered_on,
        c.has_email,
        c.has_phone,
        p.first_purchase_on,
        p.last_purchase_on,
        coalesce(p.transactions, 0)  as transactions,
        coalesce(p.units_bought, 0)  as units_bought,
        coalesce(p.total_spent, 0)   as total_spent,
        a.as_of_date - p.last_purchase_on as days_since_last_purchase,
        a.as_of_date
    from {{ ref('stg_retailku__customers') }} as c
    cross join as_of as a
    left join purchases as p using (customer_id)
)

select
    *,
    round(total_spent::numeric / nullif(transactions, 0)) as avg_transaction_value,
    case
        when transactions = 0                                       then 'no_purchase_yet'
        when days_since_last_purchase <= 14 and transactions >= 3   then 'loyal'
        when days_since_last_purchase <= 30                         then 'active'
        when days_since_last_purchase <= 60                         then 'at_risk'
        else 'dormant'
    end as segment
from joined

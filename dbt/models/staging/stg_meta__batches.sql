-- RetailKu batches uploaded with the admin key. Demo visitors' batches never reach gold.
select
    batch_id,
    source_name,
    status,
    rows_total,
    rows_auto_fixed,
    rows_quarantined,                           -- decreases as humans resolve quarantine
    rows_total - rows_quarantined as rows_passed,
    quality_score,
    created_at                    as ingested_at
from {{ source('meta', 'batches') }}
where owner_session is null
  and source_name in ('retailku_customers', 'retailku_transactions')

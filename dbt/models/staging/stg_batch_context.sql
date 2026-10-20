-- SmartIngest does not know which branch or business day a file belongs to; the rows do.
-- Every RetailKu file holds one branch and (mostly) one day, so take the most common value per batch.
with silver_rows as (
    select _batch_id as batch_id, kode_cabang as branch_code, tgl_daftar as event_date
    from {{ source('silver', 'retailku_customers') }}
    union all
    select _batch_id, kode_cabang, tanggal_transaksi
    from {{ source('silver', 'retailku_transactions') }}
),

quarantined_rows as (
    select batch_id, data ->> 'kode_cabang' as branch_code, null::date as event_date
    from {{ source('quarantine', 'rows') }}
),

all_rows as (
    select * from silver_rows
    union all
    select * from quarantined_rows
)

select
    b.batch_id,
    b.source_name,
    mode() within group (order by r.branch_code)                    as branch_code,
    coalesce(mode() within group (order by r.event_date), b.ingested_at::date) as business_date
from {{ ref('stg_meta__batches') }} as b
left join all_rows as r using (batch_id)
group by b.batch_id, b.source_name, b.ingested_at

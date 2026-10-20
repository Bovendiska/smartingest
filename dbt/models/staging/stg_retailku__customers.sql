-- One row per member: the most recently loaded version wins (a branch re-sends a member when details change).
-- Contact details are reduced to flags here, so nothing downstream of staging holds emails or phone numbers.
with ranked as (
    select
        *,
        row_number() over (
            partition by id_pelanggan
            order by _loaded_at desc, _batch_id::text desc, _row_index desc
        ) as version_rank
    from {{ source('silver', 'retailku_customers') }}
    where id_pelanggan is not null
)

select
    id_pelanggan        as customer_id,
    kode_cabang         as branch_code,
    kota                as city,
    member              as is_member,
    tgl_daftar          as registered_on,
    mail is not null    as has_email,
    no_hp is not null   as has_phone,
    _batch_id           as batch_id,
    _loaded_at          as loaded_at
from ranked
where version_rank = 1

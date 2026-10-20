-- One row per transaction id. File-level idempotency (same file = same batch) is handled by SmartIngest;
-- this handles record-level duplicates: the same sale exported again in a later file. First load wins.
with ranked as (
    select
        *,
        row_number() over (
            partition by id_transaksi
            order by _loaded_at, _batch_id::text, _row_index
        ) as copy_rank
    from {{ source('silver', 'retailku_transactions') }}
    where id_transaksi is not null
)

select
    id_transaksi                        as transaction_id,
    kode_cabang                         as branch_code,
    nullif(id_pelanggan, 'UMUM')        as customer_id,
    coalesce(id_pelanggan = 'UMUM', false) as is_walk_in,
    tanggal_transaksi                   as transaction_date,
    produk                              as product,
    kategori                            as category,
    qty                                 as quantity,
    harga_satuan                        as unit_price,
    total                               as amount,
    metode_bayar                        as payment_method,
    _batch_id                           as batch_id,
    _loaded_at                          as loaded_at
from ranked
where copy_rank = 1

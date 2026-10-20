-- Data quality per file source, branch and business day: how much arrived, how much SmartIngest fixed
-- by itself, how much still waits for a human. Answers "which branch sends the messiest files?"
with issues as (
    select
        batch_id,
        count(*) filter (where is_open)            as open_issues,
        count(*) filter (where is_auto_fixed)      as auto_fixed_issues,
        count(*) filter (where is_human_resolved)  as human_resolved_issues
    from {{ ref('stg_meta__issues') }}
    group by batch_id
)

select
    c.source_name,
    c.branch_code,
    c.business_date,
    count(*)                                    as files,
    sum(b.rows_total)                           as rows_total,
    sum(b.rows_passed)                          as rows_passed,
    sum(b.rows_auto_fixed)                      as rows_auto_fixed,
    sum(b.rows_quarantined)                     as rows_in_quarantine,
    coalesce(sum(i.open_issues), 0)             as open_issues,
    coalesce(sum(i.auto_fixed_issues), 0)       as auto_fixed_issues,
    coalesce(sum(i.human_resolved_issues), 0)   as human_resolved_issues,
    round(100.0 * sum(b.rows_passed) / nullif(sum(b.rows_total), 0), 2) as quality_score
from {{ ref('stg_batch_context') }} as c
inner join {{ ref('stg_meta__batches') }} as b using (batch_id)
left join issues as i using (batch_id)
group by c.source_name, c.branch_code, c.business_date

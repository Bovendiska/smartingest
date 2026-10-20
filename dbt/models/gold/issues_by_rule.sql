-- Which problems happen most, where, and how they end: fixed automatically, decided by a human, or still open.
-- A high acceptance rate on a rule's suggestions is the evidence for letting it auto-fix in the future.
select
    c.source_name,
    c.branch_code,
    i.column_name,
    i.rule,
    i.confidence,
    i.detected_by,
    count(*)                                         as issues,
    count(*) filter (where i.is_auto_fixed)          as auto_fixed,
    count(*) filter (where i.is_human_resolved)      as human_resolved,
    count(*) filter (where i.is_suggestion_accepted) as suggestion_accepted,
    count(*) filter (where i.is_open)                as still_open,
    round(100.0 * count(*) filter (where i.is_suggestion_accepted)
          / nullif(count(*) filter (where i.is_human_resolved), 0), 1) as acceptance_rate_pct
from {{ ref('stg_meta__issues') }} as i
inner join {{ ref('stg_batch_context') }} as c using (batch_id)
group by c.source_name, c.branch_code, i.column_name, i.rule, i.confidence, i.detected_by

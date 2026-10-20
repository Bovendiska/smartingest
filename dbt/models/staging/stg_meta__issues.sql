select
    i.issue_id,
    i.batch_id,
    i.row_index,
    i.column_name,
    i.rule,
    i.confidence,
    i.source                                        as detected_by,   -- rule | llm
    i.status,
    i.status = 'auto_fixed'                         as is_auto_fixed,
    i.status = 'open'                               as is_open,
    i.status in ('accepted', 'edited', 'value_dropped', 'row_dropped') as is_human_resolved,
    i.status = 'accepted'                           as is_suggestion_accepted,
    i.resolved_at
from {{ source('meta', 'issues') }} as i
inner join {{ ref('stg_meta__batches') }} as b using (batch_id)

-- Privacy guard: no email or phone columns may ever appear in the gold schema.
select table_name, column_name
from information_schema.columns
where table_schema = 'gold'
  and (column_name ~ '(mail|phone|no_hp|telp|whatsapp)' and column_name not like 'has\_%')

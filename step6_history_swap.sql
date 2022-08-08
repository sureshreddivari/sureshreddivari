
RENAME TABLE
  `history` TO `history_int`,
  `history_bigint` TO `history`;


DROP TRIGGER IF EXISTS sync_update_history;
-- DROP TRIGGER IF EXISTS sync_insert_history;
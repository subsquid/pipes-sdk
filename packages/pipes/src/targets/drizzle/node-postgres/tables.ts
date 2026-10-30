import { Table } from './postgres-state.js'

export function tableNotExists(error: any) {
  if (!(error instanceof Error)) return false

  if ('code' in error && error.code === '42P01') {
    return true
  } else if ('cause' in error) {
    return tableNotExists(error.cause)
  }

  return false
}

export const syncTable = (table: Table) => `
  CREATE TABLE IF NOT EXISTS ${table.fqnName}
  (
      id                      text not null,
      current_number          numeric not null,
      current_hash            text    not null,
      "current_timestamp"     timestamptz,
      finalized               jsonb,
      rollback_chain          jsonb,
      CONSTRAINT "${table.name}_pk" PRIMARY KEY("id", "current_number")
  );
  
  COMMENT ON COLUMN ${table.fqnName}."id" IS
      'Stream identifier used to separate state records within the same table.';
  
  COMMENT ON COLUMN ${table.fqnName}."current_number" IS
      'The block number of the current processed block. Acts as part of the primary key.';
  
  COMMENT ON COLUMN ${table.fqnName}."current_hash" IS
      'The block hash of the current processed block. Used together with current_number to uniquely identify the block.';
  
  COMMENT ON COLUMN ${table.fqnName}."current_timestamp" IS
      'Block time of the current processed block. NULL when the portal reported none or a value no timestamp can hold.';
  
  COMMENT ON COLUMN ${table.fqnName}."finalized" IS
      'JSON structure representing the latest finalized block returned by the chain head.';
  
  COMMENT ON COLUMN ${table.fqnName}."rollback_chain" IS
      'JSON array of BlockCursor entries used for detecting forks and reconstructing rollback points.';
`

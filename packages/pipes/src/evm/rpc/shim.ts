/**
 * Reshape `toJSON(normalized)` wire JSON so Pipes' `getBlockSchema` tagged-union accepts it. The
 * normalized model (`@subsquid/evm-normalization`) and the Portal wire schema are the same SQD EVM
 * schema except for two enumerable trace-level differences (the same shim the Squid RPC source
 * applies):
 *  - the suicide trace tag: normalized `'selfdestruct'` → schema `'suicide'`;
 *  - the reward action field: normalized `action.rewardType` → schema `action.type`.
 * Mutates in place (runs on fresh `toJSON` output).
 */
export function shimWireBlock(block: any): any {
  const traces = block?.traces
  if (Array.isArray(traces)) {
    for (const trace of traces) {
      if (trace.type === 'selfdestruct') {
        trace.type = 'suicide'
      } else if (
        trace.type === 'reward' &&
        typeof trace.action === 'object' &&
        trace.action !== null &&
        'rewardType' in trace.action
      ) {
        trace.action.type = trace.action.rewardType
        delete trace.action.rewardType
      }
    }
  }

  return block
}

CREATE TABLE IF NOT EXISTS uniswap_v3_swaps (
    block_number UInt32,
    tx_hash String,
    log_index UInt32,
    timestamp DateTime,
    pool String,
    token0 String,
    token1 String,
    amount0 Int256,
    amount1 Int256,
    sqrt_price_x96 UInt256,
    liquidity UInt256,
    tick Int64,
    sign Int8 DEFAULT 1,
    INDEX _sqd_rollback_idx block_number TYPE minmax GRANULARITY 1
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY (block_number, tx_hash, log_index)
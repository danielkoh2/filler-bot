package bots

import "time"

const MAX_TX_PACK_SIZE = 1230              //1232;
const CU_PER_FILL = 260_000                // CU cost for a successful fill
const BURST_CU_PER_FILL = 350_000          // CU cost for a successful fill
const MAX_CU_PER_TX = 1_400_000            // seems like this is all budget program gives us...on devnet
const TX_COUNT_COOLDOWN_ON_BURST = 10      // send this many tx before resetting burst mode
const FILL_ORDER_THROTTLE_BACKOFF = 10_000 // the time to wait before trying to fill a throttled (error filling) node again
const THROTTLED_NODE_SIZE_TO_PRUNE = 10    // Size of throttled nodes to get to before pruning the map
const TRIGGER_ORDER_COOLDOWN_MS = 1000     // the time to wait before trying to a node in the triggering map again
const MAX_MAKERS_PER_FILL = 6              // max number of unique makers to include per fill
const MAX_ACCOUNTS_PER_TX = 64             // solana limit, track https://github.com/solana-labs/solana/issues/27241

const SETTLE_POSITIVE_PNL_COOLDOWN_MS = 60_000

const SIM_CU_ESTIMATE_MULTIPLIER = 10.0

const TX_TIMEOUT_THRESHOLD_MS = 60 * time.Second              // tx considered stale after this time and give up confirming
const CONFIRM_TX_RATE_LIMIT_BACKOFF_MS = 5 * time.Millisecond // wait this long until trying to confirm tx again if rate limited
const CACHED_BLOCKHASH_OFFSET = 5
const DUMP_TXS_IN_SIM = false

const FillerPollingIntervalDefault = 6000
const CacheLimitSize = 10_000

const MIN_INTERVAL_FILL_MS_DEFAULT = 50
const REBALANCE_SETTLED_PNL_THRESHOLD_DEFAULT = 20
const DLOB_UPDATE_FREQUENCY_DEFAULT = 60_000
const COMPUTE_UNIT_LIMIT = 1_400_000
const MIN_LAMPORT_DEFAULT = 500

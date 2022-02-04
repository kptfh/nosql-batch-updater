package nosql.batch.update;

import nosql.batch.update.lock.Lock;
import nosql.batch.update.wal.WriteAheadLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to run batch updates on NoSql storage. Initially it was developed for Aerospike but may be implemented for any.
 * Updates should be idempotent so WriteAheadLogCompleter can safely complete interrupted batch
 * There is 2 approaches in batch updates PRE_LOCK and POST_LOCK
 *
 * PRE_LOCK - used if you know in advance all records (keys) that should be updated
 * It takes the following steps to complete batch update
 * 1) Lock keys
 * 2) Apply updates
 * 3) Unlock keys
 *
 * POST_LOCK - used if you don't know in advance all records (keys) that should be updated.
 * It takes the following steps to complete batch update
 * 1) Prepare updates
 * 2) Lock keys
 * 3) Check expected values (to guarantee that no concurrent changes while running updates and acquiring locks)
 * 4) Apply updates
 * 5) Unlock keys
 *
 * @param <LOCKS>
 * @param <UPDATES>
 * @param <L>
 * @param <BATCH_ID>
 */
public class BatchUpdater<LOCKS, UPDATES, L extends Lock, BATCH_ID> {

    private static final Logger logger = LoggerFactory.getLogger(BatchUpdater.class);

    private final WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager;
    private BatchOperations<LOCKS, UPDATES, L, BATCH_ID> batchOperations;

    public BatchUpdater(BatchOperations<LOCKS, UPDATES, L, BATCH_ID> batchOperations) {
        this.batchOperations = batchOperations;
        this.writeAheadLogManager = batchOperations.getWriteAheadLogManager();
    }

    public void update(BatchUpdate<LOCKS, UPDATES> batchUpdate) {
        BATCH_ID batchId = writeAheadLogManager.writeBatch(batchUpdate);
        logger.trace("Added batch to WAL: {}", batchId);
        batchOperations.processAndDeleteTransaction(batchId, batchUpdate, false);
    }

}

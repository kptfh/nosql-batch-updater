package nosql.batch.update.reactor;

import nosql.batch.update.BatchUpdate;
import nosql.batch.update.lock.Lock;
import nosql.batch.update.reactor.wal.ReactorWriteAheadLogManager;
import reactor.core.publisher.Mono;

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
public class ReactorBatchUpdater<LOCKS, UPDATES, L extends Lock, BATCH_ID> {

    private final ReactorWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager;
    private final ReactorBatchOperations<LOCKS, UPDATES, L, BATCH_ID> batchOperations;

    public ReactorBatchUpdater(ReactorBatchOperations<LOCKS, UPDATES, L, BATCH_ID> batchOperations) {
        this.batchOperations = batchOperations;
        this.writeAheadLogManager = batchOperations.getWriteAheadLogManager();
    }

    public Mono<Void> update(BatchUpdate<LOCKS, UPDATES> batchUpdate) {
        return writeAheadLogManager.writeBatch(batchUpdate)
                .flatMap(batchId -> batchOperations.processAndDeleteTransaction(batchId, batchUpdate, false));
    }

}

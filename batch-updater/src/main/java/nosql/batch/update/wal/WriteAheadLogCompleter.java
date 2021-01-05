package nosql.batch.update.wal;

import nosql.batch.update.BatchOperations;
import nosql.batch.update.lock.Lock;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Completes hanged transactions
 */
public class WriteAheadLogCompleter<LOCKS, UPDATES, L extends Lock, BATCH_ID>
        extends AbstractWriteAheadLogCompleter<LOCKS, UPDATES, BATCH_ID>{

    private final WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager;
    private final BatchOperations<LOCKS, UPDATES, L, BATCH_ID> batchOperations;

    /**
     * @param batchOperations
     * @param staleBatchesThreshold
     * @param exclusiveLocker
     * @param scheduledExecutorService
     */
    public WriteAheadLogCompleter(BatchOperations<LOCKS, UPDATES, L, BATCH_ID> batchOperations,
                                  Duration staleBatchesThreshold,
                                  ExclusiveLocker exclusiveLocker, ScheduledExecutorService scheduledExecutorService){
        super(staleBatchesThreshold, exclusiveLocker, scheduledExecutorService);
        this.writeAheadLogManager = batchOperations.getWriteAheadLogManager();
        this.batchOperations = batchOperations;
    }

    @Override
    protected void releaseLocksAndDeleteWalTransactionOnError(WalRecord<LOCKS, UPDATES, BATCH_ID> batch) {
        batchOperations.releaseLocksAndDeleteWalTransactionOnError(
                batch.batchUpdate.locks(), batch.batchId);
    }

    @Override
    protected void processAndDeleteTransactions(WalRecord<LOCKS, UPDATES, BATCH_ID> batch) {
        batchOperations.processAndDeleteTransaction(
                batch.batchId, batch.batchUpdate, true);
    }

    @Override
    protected List<WalRecord<LOCKS, UPDATES, BATCH_ID>> getStaleBatches(Duration staleBatchesThreshold) {
        return writeAheadLogManager.getStaleBatches(staleBatchesThreshold);
    }

}

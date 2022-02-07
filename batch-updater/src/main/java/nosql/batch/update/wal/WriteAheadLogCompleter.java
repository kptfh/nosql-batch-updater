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
                                  int batchSize,
                                  ExclusiveLocker exclusiveLocker, ScheduledExecutorService scheduledExecutorService){
        super(staleBatchesThreshold, batchSize, exclusiveLocker, scheduledExecutorService);
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
    protected List<WalTimeRange> getTimeRanges(Duration staleBatchesThreshold, int batchSize) {
        return writeAheadLogManager.getTimeRanges(staleBatchesThreshold, batchSize);
    }

    @Override
    protected List<WalRecord<LOCKS, UPDATES, BATCH_ID>> getStaleBatchesForRange(WalTimeRange timeRange) {
        return writeAheadLogManager.getStaleBatchesForRange(timeRange);
    }

}

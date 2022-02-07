package nosql.batch.update.reactor.wal;

import nosql.batch.update.lock.Lock;
import nosql.batch.update.reactor.ReactorBatchOperations;
import nosql.batch.update.wal.AbstractWriteAheadLogCompleter;
import nosql.batch.update.wal.ExclusiveLocker;
import nosql.batch.update.wal.WalRecord;
import nosql.batch.update.wal.WalTimeRange;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Completes hanged transactions
 */
public class ReactorWriteAheadLogCompleter<LOCKS, UPDATES, L extends Lock, BATCH_ID>
        extends AbstractWriteAheadLogCompleter<LOCKS, UPDATES, BATCH_ID> {

    private final ReactorWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager;
    private final ReactorBatchOperations<LOCKS, UPDATES, L, BATCH_ID> batchOperations;

    /**
     * @param batchOperations
     * @param staleBatchesThreshold
     * @param exclusiveLocker
     * @param scheduledExecutorService
     */
    public ReactorWriteAheadLogCompleter(ReactorBatchOperations<LOCKS, UPDATES, L, BATCH_ID> batchOperations,
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
                batch.batchUpdate.locks(), batch.batchId).block();
    }

    @Override
    protected void processAndDeleteTransactions(WalRecord<LOCKS, UPDATES, BATCH_ID> batch) {
        batchOperations.processAndDeleteTransaction(
                batch.batchId, batch.batchUpdate, true).block();
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

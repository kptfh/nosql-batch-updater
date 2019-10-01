package nosql.batch.update;

import nosql.batch.update.lock.Lock;
import nosql.batch.update.lock.LockOperations;
import nosql.batch.update.wal.WriteAheadLogManager;

import java.util.Collection;
import java.util.List;

public class BatchOperations<LOCKS, UPDATES, L extends Lock, BATCH_ID> {

    private final WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager;
    private final LockOperations<LOCKS, L, BATCH_ID> lockOperations;
    private final UpdateOperations<UPDATES> updateOperations;

    public BatchOperations(WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager,
                           LockOperations<LOCKS, L, BATCH_ID> lockOperations,
                           UpdateOperations<UPDATES> updateOperations) {
        this.writeAheadLogManager = writeAheadLogManager;
        this.lockOperations = lockOperations;
        this.updateOperations = updateOperations;
    }

    public void processAndDeleteTransaction(BATCH_ID batchId, BatchUpdate<LOCKS, UPDATES> batchUpdate, boolean checkTransactionId) {
        List<L> locked = lockOperations.acquire(batchId, batchUpdate.locks(), checkTransactionId,
                locksToRelease -> releaseLocksAndDeleteWalTransaction(locksToRelease, batchId));

        updateOperations.updateMany(batchUpdate.updates());
        releaseLocksAndDeleteWalTransaction(locked, batchId);
    }

    private void releaseLocksAndDeleteWalTransaction(Collection<L> locks, BATCH_ID batchId) {
        lockOperations.release(locks, batchId)
                .then(writeAheadLogManager.deleteBatch(batchId))
                .subscribe();
    }

    public void releaseLocksAndDeleteWalTransactionOnError(LOCKS locks, BATCH_ID batchId) {
        List<L> transactionLockKeys = lockOperations.getLockedByBatchUpdate(locks, batchId);
        releaseLocksAndDeleteWalTransaction(transactionLockKeys, batchId);
    }

    public WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> getWriteAheadLogManager() {
        return writeAheadLogManager;
    }

}

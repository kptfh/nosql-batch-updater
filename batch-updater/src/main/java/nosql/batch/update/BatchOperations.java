package nosql.batch.update;

import nosql.batch.update.lock.Lock;
import nosql.batch.update.lock.LockOperations;
import nosql.batch.update.wal.TransactionId;
import nosql.batch.update.wal.WriteAheadLogManager;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class BatchOperations<L, U> {

    private final WriteAheadLogManager<L, U> writeAheadLogManager;
    private final LockOperations<L> lockOperations;
    private final UpdateOperations<U> updateOperations;

    public BatchOperations(WriteAheadLogManager<L, U> writeAheadLogManager,
                           LockOperations<L> lockOperations,
                           UpdateOperations<U> updateOperations) {
        this.writeAheadLogManager = writeAheadLogManager;
        this.lockOperations = lockOperations;
        this.updateOperations = updateOperations;
    }

    public void processAndDeleteTransaction(TransactionId transactionId, BatchUpdate<L, U> batchUpdate, boolean checkTransactionId) {
        Set<Lock> locked = lockOperations.acquire(transactionId, batchUpdate.locks(), checkTransactionId,
                locksToRelease -> releaseLocksAndDeleteWalTransaction(locksToRelease, transactionId));

        updateOperations.updateMany(batchUpdate.updates());
        releaseLocksAndDeleteWalTransaction(locked, transactionId);
    }

    private void releaseLocksAndDeleteWalTransaction(Collection<Lock> locks, TransactionId transactionId) {
        lockOperations.release(locks);
        writeAheadLogManager.deleteTransaction(transactionId);
    }

    public void releaseLocksAndDeleteWalTransactionOnError(L locks, TransactionId transactionId) {
        List<Lock> transactionLockKeys = lockOperations.getLockedByTransaction(locks, transactionId);
        releaseLocksAndDeleteWalTransaction(transactionLockKeys, transactionId);
    }

    public WriteAheadLogManager<L, U> getWriteAheadLogManager() {
        return writeAheadLogManager;
    }

}

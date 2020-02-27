package nosql.batch.update;

import nosql.batch.update.lock.Lock;
import nosql.batch.update.lock.LockOperations;
import nosql.batch.update.wal.WriteAheadLogManager;
import reactor.core.publisher.Mono;

import java.util.Collection;

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

    public Mono<Void> processAndDeleteTransaction(BATCH_ID batchId, BatchUpdate<LOCKS, UPDATES> batchUpdate, boolean checkTransactionId) {
        return lockOperations.acquire(batchId, batchUpdate.locks(), checkTransactionId,
                locksToRelease -> releaseLocksAndDeleteWalTransactionOnError(batchUpdate.locks(), batchId))
                .flatMap(locked -> updateOperations.updateMany(batchUpdate.updates())
                        .then(releaseLocksAndDeleteWalTransaction(locked, batchId)));
    }

    private Mono<Void> releaseLocksAndDeleteWalTransaction(Collection<L> locks, BATCH_ID batchId) {
        return lockOperations.release(locks, batchId)
                //here we use fire&forget to reduce response time
                .doOnSuccess(aVoid -> writeAheadLogManager.deleteBatch(batchId).subscribe());
    }

    public Mono<Void> releaseLocksAndDeleteWalTransactionOnError(LOCKS locks, BATCH_ID batchId) {
        return lockOperations.getLockedByBatchUpdate(locks, batchId)
                .flatMap(transactionLockKeys -> releaseLocksAndDeleteWalTransaction(transactionLockKeys, batchId));
    }

    public WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> getWriteAheadLogManager() {
        return writeAheadLogManager;
    }

}

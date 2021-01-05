package nosql.batch.update;

import nosql.batch.update.lock.Lock;
import nosql.batch.update.lock.LockOperations;
import nosql.batch.update.wal.WriteAheadLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class BatchOperations<LOCKS, UPDATES, L extends Lock, BATCH_ID> {

    private static final Logger logger = LoggerFactory.getLogger(BatchOperations.class);

    private final WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager;
    private final LockOperations<LOCKS, L, BATCH_ID> lockOperations;
    private final UpdateOperations<UPDATES> updateOperations;
    private final ExecutorService executorService;

    public BatchOperations(WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager,
                           LockOperations<LOCKS, L, BATCH_ID> lockOperations,
                           UpdateOperations<UPDATES> updateOperations,
                           ExecutorService executorService) {
        this.writeAheadLogManager = writeAheadLogManager;
        this.lockOperations = lockOperations;
        this.updateOperations = updateOperations;
        this.executorService = executorService;
    }

    public void processAndDeleteTransaction(BATCH_ID batchId, BatchUpdate<LOCKS, UPDATES> batchUpdate, boolean calledByWal) {
        List<L> locksAcquired;
        try {
            locksAcquired = lockOperations.acquire(batchId, batchUpdate.locks(), calledByWal);
        } catch (Throwable t) {
            if (logger.isTraceEnabled()) {
                logger.trace("Failed to acquire locks [{}] batchId=[{}]. Will release locks", batchId, batchUpdate.locks());
            }
            releaseLocksAndDeleteWalTransactionOnError(batchUpdate.locks(), batchId);
            throw t;
        }

        updateOperations.updateMany(batchUpdate.updates(), calledByWal);

        if(logger.isTraceEnabled()){
            logger.trace("Applied updates [{}] batchId=[{}]", batchId, batchUpdate);
        }

        releaseLocksAndDeleteWalTransaction(locksAcquired, batchId);
    }

    private void releaseLocksAndDeleteWalTransaction(Collection<L> locks, BATCH_ID batchId) {
        lockOperations.release(locks, batchId);
        //here we use fire&forget to reduce response time
        executorService.submit(() -> {
            try {
                boolean deleted = writeAheadLogManager.deleteBatch(batchId);
                if(deleted) {
                    logger.trace("Removed batch from WAL: {}", batchId);
                } else {
                    logger.error("Missed batch in WAL: {}", batchId);
                }
            } catch (Throwable t) {
                logger.error("Error while removing batch from WAL", t);
            }
        });
    }

    public void releaseLocksAndDeleteWalTransactionOnError(LOCKS locks, BATCH_ID batchId) {
        List<L> transactionLockKeys = lockOperations.getLockedByBatchUpdate(locks, batchId);
        releaseLocksAndDeleteWalTransaction(transactionLockKeys, batchId);
    }

    public WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> getWriteAheadLogManager() {
        return writeAheadLogManager;
    }

}

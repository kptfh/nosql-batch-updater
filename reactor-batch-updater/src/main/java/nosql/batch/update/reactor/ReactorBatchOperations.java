package nosql.batch.update.reactor;

import nosql.batch.update.BatchUpdate;
import nosql.batch.update.lock.Lock;
import nosql.batch.update.lock.LockingException;
import nosql.batch.update.reactor.lock.ReactorLockOperations;
import nosql.batch.update.reactor.wal.ReactorWriteAheadLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Collection;

public class ReactorBatchOperations<LOCKS, UPDATES, L extends Lock, BATCH_ID> {

    private static final Logger logger = LoggerFactory.getLogger(ReactorBatchOperations.class);

    private final ReactorWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager;
    private final ReactorLockOperations<LOCKS, L, BATCH_ID> lockOperations;
    private final ReactorUpdateOperations<UPDATES> updateOperations;

    public ReactorBatchOperations(ReactorWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager,
                                  ReactorLockOperations<LOCKS, L, BATCH_ID> lockOperations,
                                  ReactorUpdateOperations<UPDATES> updateOperations) {
        this.writeAheadLogManager = writeAheadLogManager;
        this.lockOperations = lockOperations;
        this.updateOperations = updateOperations;
    }

    public Mono<Void> processAndDeleteTransaction(BATCH_ID batchId, BatchUpdate<LOCKS, UPDATES> batchUpdate, boolean calledByWal) {
        return lockOperations.acquire(batchId, batchUpdate.locks(), calledByWal)
                /*.onErrorResume(throwable -> onErrorCleaner.apply(batchLocks)
                            .then(Mono.error(throwable)))*/
                .doOnError(LockingException.class, throwable -> {
                    if(logger.isTraceEnabled()){
                        logger.trace("Failed to acquire locks [{}] batchId=[{}]. Will release locks", batchId, batchUpdate.locks());
                    }
                    releaseLocksAndDeleteWalTransactionOnError(batchUpdate.locks(), batchId)
                            .subscribe();
                })
                .flatMap(locked -> updateOperations.updateMany(batchUpdate.updates(), calledByWal)
                        .doOnSuccess(unused -> {
                            if(logger.isTraceEnabled()){
                                logger.trace("Applied updates [{}] batchId=[{}]", batchId, batchUpdate);
                            }
                        })
                        .then(releaseLocksAndDeleteWalTransaction(locked, batchId)));
    }

    private Mono<Void> releaseLocksAndDeleteWalTransaction(Collection<L> locks, BATCH_ID batchId) {
        return lockOperations.release(locks, batchId)
                //here we use fire&forget to reduce response time
                .doOnSuccess(aVoid -> writeAheadLogManager.deleteBatch(batchId)
                        .doOnNext(deleted -> {
                            if(deleted) {
                                logger.trace("Removed batch from WAL: {}", batchId);
                            } else {
                                logger.error("Missed batch in WAL: {}", batchId);
                            }
                        })
                        .subscribe());
    }

    public Mono<Void> releaseLocksAndDeleteWalTransactionOnError(LOCKS locks, BATCH_ID batchId) {
        return lockOperations.getLockedByBatchUpdate(locks, batchId)
                .flatMap(transactionLockKeys -> releaseLocksAndDeleteWalTransaction(transactionLockKeys, batchId));
    }

    public ReactorWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> getWriteAheadLogManager() {
        return writeAheadLogManager;
    }

}

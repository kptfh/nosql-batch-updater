package nosql.batch.update.reactor.lock;

import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static nosql.batch.update.reactor.util.HangingUtil.hang;

abstract public class HangingLockOperations<LOCKS, L extends Lock, BATCH_ID> implements LockOperations<LOCKS, L, BATCH_ID> {

    private final LockOperations<LOCKS, L, BATCH_ID> lockOperations;
    private final AtomicBoolean failsAcquire;
    private final AtomicBoolean failsRelease;

    public HangingLockOperations(LockOperations<LOCKS, L, BATCH_ID> lockOperations,
                                 AtomicBoolean failsAcquire, AtomicBoolean failsRelease) {
        this.lockOperations = lockOperations;
        this.failsAcquire = failsAcquire;
        this.failsRelease = failsRelease;
    }

    abstract protected LOCKS selectFlakingToAcquire(LOCKS locks);
    abstract protected Collection<L> selectFlakingToRelease(Collection<L> locks);

    @Override
    public Mono<List<L>> acquire(BATCH_ID batchId, LOCKS locks, boolean checkTransactionId,
                                 Function<LOCKS, Mono<Void>> onErrorCleaner) throws LockingException {
        if(failsAcquire.get()) {
            LOCKS partialLocks = selectFlakingToAcquire(locks);

            return lockOperations.acquire(batchId, partialLocks, checkTransactionId, onErrorCleaner)
                    .then(hang());
        } else {
            return lockOperations.acquire(batchId, locks, checkTransactionId, onErrorCleaner);
        }
    }

    @Override
    public Mono<List<L>> getLockedByBatchUpdate(LOCKS locks, BATCH_ID batchId) {
        return lockOperations.getLockedByBatchUpdate(locks, batchId);
    }

    @Override
    public Mono<Void> release(Collection<L> locks, BATCH_ID batchId) {
        if(failsRelease.get()){
            Collection<L> partialLocks = selectFlakingToRelease(locks);
            return lockOperations.release(partialLocks, batchId)
                    .then(hang());
        } else {
            return lockOperations.release(locks, batchId);
        }
    }

}
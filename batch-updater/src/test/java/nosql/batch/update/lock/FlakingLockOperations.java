package nosql.batch.update.lock;

import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

abstract public class FlakingLockOperations<LOCKS, L extends Lock, BATCH_ID> implements LockOperations<LOCKS, L, BATCH_ID> {

    private final LockOperations<LOCKS, L, BATCH_ID> lockOperations;
    private final AtomicBoolean failsAcquire;
    private final AtomicBoolean failsRelease;

    public FlakingLockOperations(LockOperations<LOCKS, L, BATCH_ID> lockOperations, AtomicBoolean failsAcquire, AtomicBoolean failsRelease) {
        this.lockOperations = lockOperations;
        this.failsAcquire = failsAcquire;
        this.failsRelease = failsRelease;
    }

    abstract protected LOCKS selectFlakingToAcquire(LOCKS locks);
    abstract protected Collection<L> selectFlakingToRelease(Collection<L> locks);

    @Override
    public List<L> acquire(BATCH_ID batchId, LOCKS locks, boolean checkTransactionId, Consumer<Collection<L>> onErrorCleaner) throws LockingException {
        if(failsAcquire.get()) {
            LOCKS partialLocks = selectFlakingToAcquire(locks);

            lockOperations.acquire(batchId, partialLocks, checkTransactionId, collection -> {});

            throw new RuntimeException();
        } else {
            return lockOperations.acquire(batchId, locks, checkTransactionId, onErrorCleaner);
        }
    }

    @Override
    public List<L> getLockedByBatchUpdate(LOCKS locks, BATCH_ID batchId) {
        return lockOperations.getLockedByBatchUpdate(locks, batchId);
    }

    @Override
    public Mono<Void> release(Collection<L> locks, BATCH_ID batchId) {
        if(failsRelease.get()){
            Collection<L> partialLocks = selectFlakingToRelease(locks);
            return lockOperations.release(partialLocks, batchId)
                    .then(Mono.error(new RuntimeException()));
        } else {
            return lockOperations.release(locks, batchId);
        }
    }

}
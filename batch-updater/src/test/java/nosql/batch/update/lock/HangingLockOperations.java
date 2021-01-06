package nosql.batch.update.lock;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.util.HangingUtil.hang;

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
    abstract protected List<L> selectFlakingToRelease(List<L> locks);

    @Override
    public List<L> acquire(BATCH_ID batchId, LOCKS locks, boolean checkTransactionId) throws LockingException {
        if(failsAcquire.get()) {
            LOCKS partialLocks = selectFlakingToAcquire(locks);
            try {
                return lockOperations.acquire(batchId, partialLocks, checkTransactionId);
            } finally {
                hang();
            }
        } else {
            return lockOperations.acquire(batchId, locks, checkTransactionId);
        }
    }

    @Override
    public List<L> getLockedByBatchUpdate(LOCKS locks, BATCH_ID batchId) {
        return lockOperations.getLockedByBatchUpdate(locks, batchId);
    }

    @Override
    public void release(List<L> locks, BATCH_ID batchId) {
        if(failsRelease.get()){
            List<L> partialLocks = selectFlakingToRelease(locks);
            try {
                lockOperations.release(partialLocks, batchId);
            } finally {
                hang();
            }
        } else {
            lockOperations.release(locks, batchId);
        }
    }

}
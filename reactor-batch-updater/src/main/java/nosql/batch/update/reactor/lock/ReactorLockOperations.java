package nosql.batch.update.reactor.lock;

import nosql.batch.update.lock.Lock;
import nosql.batch.update.lock.LockingException;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;

public interface ReactorLockOperations<LOCKS, L extends Lock, BATCH_ID> {

    /**
     *
     * @param batchId
     * @param locks
     * @param checkBatchId
     *
     * @return
     */
    Mono<List<L>> acquire(BATCH_ID batchId,
                    LOCKS locks, boolean checkBatchId) throws LockingException;

    Mono<List<L>> getLockedByBatchUpdate(LOCKS locks, BATCH_ID batchId);

    Mono<Void> release(Collection<L> locks, BATCH_ID batchId);
}

package nosql.batch.update.lock;

import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

public interface LockOperations<LOCKS, L extends Lock, BATCH_ID> {

    /**
     *
     * @param batchId
     * @param locks
     * @param checkBatchId
     *
     * @param onErrorCleaner In case we were not able to get all locks we should clean(unlock) them
     * @return
     */
    List<L> acquire(BATCH_ID batchId,
                    LOCKS locks, boolean checkBatchId,
                    Consumer<Collection<L>> onErrorCleaner) throws LockingException;

    List<L> getLockedByBatchUpdate(LOCKS locks, BATCH_ID batchId);

    Mono<Void> release(Collection<L> locks, BATCH_ID batchId);
}

package nosql.batch.update.lock;

import java.util.Collection;
import java.util.List;

public interface LockOperations<LOCKS, L extends Lock, BATCH_ID> {

    /**
     *
     * @param batchId
     * @param locks
     * @param checkBatchId
     * @return
     */
    List<L> acquire(BATCH_ID batchId,
                    LOCKS locks, boolean checkBatchId) throws LockingException;

    List<L> getLockedByBatchUpdate(LOCKS locks, BATCH_ID batchId);

    void release(Collection<L> locks, BATCH_ID batchId);
}

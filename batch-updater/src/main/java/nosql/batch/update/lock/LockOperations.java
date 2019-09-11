package nosql.batch.update.lock;

import nosql.batch.update.wal.TransactionId;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public interface LockOperations<L> {

    /**
     *
     * @param transactionId
     * @param locks
     * @param checkTransactionId
     *
     * @param onErrorCleaner In case we were not able to get all locks we should clean(unlock) them
     * @return
     */
    Set<Lock> acquire(TransactionId transactionId,
                      L locks, boolean checkTransactionId,
                      Consumer<Set<Lock>> onErrorCleaner);

    List<Lock> getLockedByTransaction(L locks, TransactionId transactionId);

    void release(Collection<Lock> locks);
}

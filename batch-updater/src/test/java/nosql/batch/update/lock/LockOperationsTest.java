package nosql.batch.update.lock;

import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

abstract public class LockOperationsTest<LOCKS, L extends Lock, BATCH_ID> {

    private final LockOperations<LOCKS, L, BATCH_ID> lockOperations;

    public LockOperationsTest(LockOperations<LOCKS, L, BATCH_ID> lockOperations) {
        this.lockOperations = lockOperations;
    }

    abstract protected LOCKS getLocks1();
    abstract protected BATCH_ID generateBatchId();
    abstract protected void assertThatSameLockKeys(List<L> locks1, List<L> locks2);

    @Test
    public void shouldNotLockLocked(){
        BATCH_ID batchId1 = generateBatchId();
        List<L> acquiredLocks = lockOperations.acquire(batchId1, getLocks1(), false);
        assertThat(acquiredLocks).isNotEmpty();
        assertThat(acquiredLocks.stream().map(l -> l.lockType).collect(Collectors.toSet()))
                .containsExactly(Lock.LockType.LOCKED);

        assertThatThrownBy(() -> lockOperations.acquire(generateBatchId(), getLocks1(), false))
           .isInstanceOf(TemporaryLockingException.class);

        lockOperations.release(acquiredLocks, batchId1);

        List<L> acquiredLocks1 = lockOperations.acquire(batchId1, getLocks1(), false);
        assertThat(acquiredLocks1).containsExactlyInAnyOrderElementsOf(acquiredLocks);
    }

    @Test
    public void shouldLockLockedForSameBatch(){
        BATCH_ID batchId1 = generateBatchId();
        List<L> acquiredLocks = lockOperations.acquire(batchId1, getLocks1(), false);
        assertThat(acquiredLocks).isNotEmpty();
        assertThat(acquiredLocks.stream().map(l -> l.lockType).collect(Collectors.toSet()))
                .containsExactly(Lock.LockType.LOCKED);

        List<L> acquiredLocks1 = lockOperations.acquire(batchId1, getLocks1(), true);

        assertThatSameLockKeys(acquiredLocks1, acquiredLocks);
        assertThat(acquiredLocks1.stream().map(l -> l.lockType).collect(Collectors.toSet()))
                .containsExactly(Lock.LockType.SAME_BATCH);
    }

    @Test
    public void shouldReturnLocked(){
        BATCH_ID batchId1 = generateBatchId();
        List<L> acquiredLocks = lockOperations.acquire(batchId1, getLocks1(), false);

        List<L> lockedLocks = lockOperations.getLockedByBatchUpdate(getLocks1(), batchId1);

        assertThatSameLockKeys(lockedLocks, acquiredLocks);
        assertThat(lockedLocks.stream().map(l -> l.lockType).collect(Collectors.toSet()))
                .containsExactly(Lock.LockType.SAME_BATCH);
    }

}

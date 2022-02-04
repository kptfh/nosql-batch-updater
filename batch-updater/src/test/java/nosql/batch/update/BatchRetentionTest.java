package nosql.batch.update;

import nosql.batch.update.lock.PermanentLockingException;
import nosql.batch.update.lock.TemporaryLockingException;
import nosql.batch.update.wal.CompletionStatistic;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static nosql.batch.update.RecoveryTest.completionStatisticAssertion;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Checks whether transaction
 *  - removed on failed lock
 *  - retains on failed mutation
 */
abstract public class BatchRetentionTest {

    protected abstract void cleanUp() throws InterruptedException;
    protected abstract void runUpdate();

    protected abstract CompletionStatistic runCompleter();
    protected abstract void checkForConsistency();

    protected static final AtomicReference<Throwable> failsAcquireLock = new AtomicReference<>();
    protected static final AtomicReference<Throwable> failsCheckValue = new AtomicReference<>();
    protected static final AtomicBoolean failsMutate = new AtomicBoolean();
    protected static final AtomicBoolean failsReleaseLock = new AtomicBoolean();
    protected static final AtomicBoolean failsDeleteBatch = new AtomicBoolean();
    protected static final AtomicInteger deletesInProcess = new AtomicInteger();

    @Test
    public void shouldKeepConsistencyIfAcquireFailed() throws InterruptedException {
        shouldBecameConsistentAfterFailAndCompletion(() -> failsAcquireLock.set(new RuntimeException()), RuntimeException.class,
                completionStatisticAssertion(1, 1, 0));
    }

    @Test
    public void shouldKeepConsistencyIfAcquireFailedWithLockingException() throws InterruptedException {
        shouldBecameConsistentAfterFailAndCompletion(() -> failsAcquireLock.set(new TemporaryLockingException("test")), RuntimeException.class,
                completionStatisticAssertion(0, 0, 0));
    }

    @Test
    public void shouldKeepConsistencyIfCheckValueFailed() throws InterruptedException {
        shouldBecameConsistentAfterFailAndCompletion(() -> failsCheckValue.set(new RuntimeException()), RuntimeException.class,
                completionStatisticAssertion(1, 1, 0));
    }

    @Test
    public void shouldKeepConsistencyIfCheckValueFailedWithLockingExcption() throws InterruptedException {
        shouldBecameConsistentAfterFailAndCompletion(() -> failsCheckValue.set(new PermanentLockingException("test")), RuntimeException.class,
                completionStatisticAssertion(0, 0, 0));
    }

    @Test
    public void shouldKeepConsistencyIfMutationFailed() throws InterruptedException {
        shouldBecameConsistentAfterFailAndCompletion(() -> failsMutate.set(true), RuntimeException.class,
                completionStatisticAssertion(1, 1, 0));
    }

    @Test
    public void shouldKeepConsistencyIfReleaseFailed() throws InterruptedException {
        shouldBecameConsistentAfterFailAndCompletion(() -> failsReleaseLock.set(true), RuntimeException.class,
                completionStatisticAssertion(1));
    }

    @Test
    public void shouldRetainBatchIfDeleteBatchFailed() throws InterruptedException {
        shouldBecameConsistentAfterFailAndCompletion(() -> failsDeleteBatch.set(true), null,
                completionStatisticAssertion(10, 0, 10));
    }

    private void shouldBecameConsistentAfterFailAndCompletion(
            Runnable breaker, Class<? extends Exception> expectedException,
            Consumer<CompletionStatistic> completionStatisticAssertion) throws InterruptedException {
        for (int i = 0; i < 20; i++) {
            cleanUp();

            fixAll();

            breaker.run();

            if(expectedException != null){
                assertThatThrownBy(this::runUpdate)
                        .isInstanceOf(expectedException);
            } else {
                runUpdate();
            }

            Awaitility.waitAtMost(Duration.ONE_SECOND).until(() -> deletesInProcess.get() == 0);

            fixAll();

            CompletionStatistic completionStatistic = runCompleter();
            completionStatisticAssertion.accept(completionStatistic);

            checkForConsistency();
        }
    }

    private void fixAll(){
        failsAcquireLock.set(null);
        failsCheckValue.set(null);
        failsMutate.set(false);
        failsReleaseLock.set(false);
        failsDeleteBatch.set(false);
    }
}


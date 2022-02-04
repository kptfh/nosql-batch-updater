package nosql.batch.update;

import nosql.batch.update.wal.CompletionStatistic;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static nosql.batch.update.util.HangingUtil.hanged;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.FIVE_MINUTES;

/**
 * Check that hanged batch get recovered by WriteAheadLogCompleter
 */
abstract public class RecoveryTest {

    protected abstract void cleanUp() throws InterruptedException;

    protected abstract void runUpdate();

    protected abstract CompletionStatistic runCompleter();

    protected abstract void checkForConsistency();

    protected static final AtomicBoolean hangsAcquire = new AtomicBoolean();
    protected static final AtomicBoolean hangsUpdate = new AtomicBoolean();
    protected static final AtomicBoolean hangsRelease = new AtomicBoolean();
    protected static final AtomicBoolean hangsDeleteBatchInWal = new AtomicBoolean();

    @Test
    public void shouldBecameConsistentAfterAcquireLockHanged() throws InterruptedException {
        shouldBecameConsistentAfterHangAndCompletion(
                () -> hangsAcquire.set(true),
                completionStatisticAssertion(1, 1, 0));
     }

    @Test
    public void shouldBecameConsistentAfterMutateHanged() throws InterruptedException {
        shouldBecameConsistentAfterHangAndCompletion(
                () -> hangsUpdate.set(true),
                completionStatisticAssertion(1, 1, 0));
    }

    @Test
    public void shouldBecameConsistentAfterReleaseLockHanged() throws InterruptedException {
        shouldBecameConsistentAfterHangAndCompletion(
                () -> hangsRelease.set(true),
                completionStatisticAssertion(1));
    }

    @Test
    public void shouldBecameConsistentAfterDeleteTransactionHanged() throws InterruptedException {
        shouldBecameConsistentAfterHangAndCompletion(
                () -> hangsDeleteBatchInWal.set(true),
                completionStatisticAssertion(
                        staleBatchesFound -> staleBatchesFound >= 1,
                        staleBatchesComplete -> staleBatchesComplete == 0,
                        staleBatchesIgnored -> staleBatchesIgnored >= 1));
    }

    protected void shouldBecameConsistentAfterHangAndCompletion(
            Runnable breaker, Consumer<CompletionStatistic> completionStatisticAssertion) throws InterruptedException {

        for(int i = 0; i < 10; i++) {
            cleanUp();

            fixAll();
            breaker.run();

            new Thread(this::runUpdate).start();

            await().dontCatchUncaughtExceptions()
                    .timeout(FIVE_MINUTES)
                    .until(hanged::get);

            fixAll();

            CompletionStatistic completionStat = runCompleter();
            completionStatisticAssertion.accept(completionStat);

            //check state. It should be fixed at this time
            checkForConsistency();

            //check normal update is possible
            runUpdate();
            checkForConsistency();
        }
    }

    protected void fixAll() {
        hangsAcquire.set(false);
        hangsUpdate.set(false);
        hangsRelease.set(false);
        hangsDeleteBatchInWal.set(false);
        hanged.set(false);
    }

    static Consumer<CompletionStatistic> completionStatisticAssertion(
            int staleBatchesFound, int staleBatchesComplete, int staleBatchesIgnored){
        return completionStatistic -> {
            assertThat(completionStatistic.staleBatchesFound).isEqualTo(staleBatchesFound);
            assertThat(completionStatistic.staleBatchesComplete).isEqualTo(staleBatchesComplete);
            assertThat(completionStatistic.staleBatchesIgnored).isEqualTo(staleBatchesIgnored);
        };
    }

    static Consumer<CompletionStatistic> completionStatisticAssertion(
            Predicate<Integer> staleBatchesFound, Predicate<Integer> staleBatchesComplete, Predicate<Integer> staleBatchesIgnored){
        return completionStatistic -> {
            assertThat(completionStatistic.staleBatchesFound).matches(staleBatchesFound);
            assertThat(completionStatistic.staleBatchesComplete).matches(staleBatchesComplete);
            assertThat(completionStatistic.staleBatchesIgnored).matches(staleBatchesIgnored);
        };
    }

    static Consumer<CompletionStatistic> completionStatisticAssertion(int staleBatchesFound){
        return completionStatistic ->
                assertThat(completionStatistic.staleBatchesFound).isEqualTo(staleBatchesFound);
    }
}

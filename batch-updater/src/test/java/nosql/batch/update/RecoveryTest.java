package nosql.batch.update;

import nosql.batch.update.util.FixedClock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

abstract public class RecoveryTest {

    private static Logger logger = LoggerFactory.getLogger(RecoveryTest.class);

    protected abstract void cleanUp() throws InterruptedException;

    protected abstract void runUpdate();

    protected abstract void runCompleter();

    protected abstract void checkForConsistency();

    protected static final AtomicBoolean failsAcquire = new AtomicBoolean();
    protected static final AtomicBoolean failsUpdate = new AtomicBoolean();
    protected static final AtomicBoolean failsRelease = new AtomicBoolean();
    protected static final AtomicBoolean failsDeleteBatchInWal = new AtomicBoolean();


    @Test
    public void shouldBecameConsistentAfterAcquireLockFailed() throws InterruptedException {
        shouldBecameConsistentAfterFailure(() -> failsAcquire.set(true));
    }

    @Test
    public void shouldBecameConsistentAfterReleaseLockFailed() throws InterruptedException {
        shouldBecameConsistentAfterFailure(() -> failsRelease.set(true));
    }

    @Test
    public void shouldBecameConsistentAfterMutateFailed() throws InterruptedException {
        shouldBecameConsistentAfterFailure(() -> failsUpdate.set(true));
    }

    @Test
    public void shouldBecameConsistentAfterDeleteTransactionFailed() throws InterruptedException {
        shouldBecameConsistentAfterFailure(() -> failsDeleteBatchInWal.set(true));
    }

    protected void shouldBecameConsistentAfterFailure(Runnable breaker) throws InterruptedException {
        for(int i = 0; i < 10; i++) {
            cleanUp();

            fixAll();
            breaker.run();

            boolean failed = false;
            try {
                runUpdate();
            } catch (Exception e) {
                //here we may got inconsistent state
                failed = true;
                logger.error("As expected failed on update");
            }

            if(failed) {
                fixAll();

                runCompleter();

                //check state. It should be fixed at this time
                checkForConsistency();
            }
        }
    }

    protected void fixAll() {
        failsAcquire.set(false);
        failsUpdate.set(false);
        failsRelease.set(false);
        failsDeleteBatchInWal.set(false);
    }

}

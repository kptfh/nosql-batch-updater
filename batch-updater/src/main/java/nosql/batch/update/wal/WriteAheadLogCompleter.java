package nosql.batch.update.wal;

import nosql.batch.update.BatchOperations;
import nosql.batch.update.lock.FailedToAcquireAllLocksException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.util.AsyncUtil.shutdownAndAwaitTermination;

/**
 * Completes hanged transactions
 */
public class WriteAheadLogCompleter<L, U> {

    private static Logger logger = LoggerFactory.getLogger(WriteAheadLogCompleter.class);

    private static final long WAIT_TIMEOUT_IN_SECONDS = 10;

    private final WriteAheadLogManager<L, U> writeAheadLogManager;
    private final Duration delayBeforeCompletion;
    private final BatchOperations<L, U> batchOperations;
    private final ExclusiveLocker exclusiveLocker;
    private final ScheduledExecutorService scheduledExecutorService;

    private AtomicBoolean suspended = new AtomicBoolean(false);

    /**
     * @param batchOperations
     * @param delayBeforeCompletion set period to be slightly longer then expiration
     * @param exclusiveLocker
     * @param scheduledExecutorService
     */
    public WriteAheadLogCompleter(BatchOperations<L, U> batchOperations, Duration delayBeforeCompletion,
                                  ExclusiveLocker exclusiveLocker, ScheduledExecutorService scheduledExecutorService){
        this.writeAheadLogManager = batchOperations.getWriteAheadLogManager();
        this.batchOperations = batchOperations;

        this.delayBeforeCompletion = delayBeforeCompletion;
        this.exclusiveLocker = exclusiveLocker;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void start(){
        scheduledExecutorService.scheduleAtFixedRate(
                this::completeHangedTransactions,
                0, delayBeforeCompletion.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void shutdown(){
        shutdownAndAwaitTermination(scheduledExecutorService, WAIT_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * You should call it when the data center had been switched into the passive mode
     */
    public void suspend(){
        this.suspended.set(true);
    }

    public boolean isSuspended(){
        return this.suspended.get();
    }

    /**
     * You should call it when the data center had been switched into the active mode
     */
    public void resume(){
        this.suspended.set(false);
    }

    private void completeHangedTransactions() {

        if(suspended.get()){
            logger.info("WAL execution was suspended");
            return;
        }

        try {
            if(exclusiveLocker.acquireExclusiveLock()){
                List<WalTransaction<L, U>> staleTransactions = writeAheadLogManager.getStaleTransactions();
                logger.info("Got {} stale transactions", staleTransactions.size());
                for(WalTransaction<L, U> transaction : staleTransactions){
                    if(suspended.get()){
                        logger.info("WAL execution was suspended");
                        break;
                    }
                    if(Thread.currentThread().isInterrupted()){
                        logger.info("WAL execution was interrupted");
                        break;
                    }

                    if(exclusiveLocker.renewExclusiveLock()) {
                        logger.info("Trying to complete transaction txId=[{}], timestamp=[{}]",
                                transaction.transactionId, transaction.timestamp);
                        try {
                            batchOperations.processAndDeleteTransaction(
                                    transaction.transactionId, transaction.locks, transaction.updates, true);
                            logger.info("Successfully complete transaction txId=[{}]", transaction.transactionId);
                        }
                        //this is expected behaviour that may have place in case of hanged transaction was not completed:
                        //not able to acquire all locks (didn't match expected value
                        // (may have place if initial transaction was interrupted on release stage and released values were modified))
                        catch (FailedToAcquireAllLocksException be) {
                            logger.info("Failed to complete transaction txId=[{}] as it's already completed", transaction.transactionId, be);
                            batchOperations.releaseLocksAndDeleteWalTransactionOnError(
                                    transaction.locks, transaction.transactionId);
                            logger.info("released locks for transaction txId=[{}]", transaction.transactionId, be);
                        }
                        //even in case of error need to move to the next one
                        catch (Exception e) {
                            logger.error("!!! Failed to complete transaction txId=[{}], need to be investigated",
                                    transaction.transactionId, e);
                        }
                    }
                }
            }
        }
        catch (Throwable t) {
            logger.error("Error while running completeHangedTransactions()", t);
            throw t;
        }
    }

}

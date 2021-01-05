package nosql.batch.update.wal;

import nosql.batch.update.lock.LockingException;
import nosql.batch.update.util.AsyncUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Completes hanged transactions
 */
abstract public class AbstractWriteAheadLogCompleter<LOCKS, UPDATES, BATCH_ID> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractWriteAheadLogCompleter.class);

    private final Duration staleBatchesThreshold;

    private final ExclusiveLocker exclusiveLocker;
    private final ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture<?> scheduledFuture;

    private final AtomicBoolean suspended = new AtomicBoolean(false);

    /**
     * @param staleBatchesThreshold
     * @param exclusiveLocker
     * @param scheduledExecutorService
     */
    public AbstractWriteAheadLogCompleter(Duration staleBatchesThreshold,
                                          ExclusiveLocker exclusiveLocker, ScheduledExecutorService scheduledExecutorService){
        this.staleBatchesThreshold = staleBatchesThreshold;
        this.exclusiveLocker = exclusiveLocker;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void start(){
        scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(
                this::completeHangedTransactions,
                //set period to be slightly longer then expiration
                0, staleBatchesThreshold.toMillis() + 100, TimeUnit.MILLISECONDS);
    }

    public void shutdown(){
        scheduledFuture.cancel(true);
        AsyncUtil.shutdownAndAwaitTermination(scheduledExecutorService);
        exclusiveLocker.release();
        exclusiveLocker.shutdown();
    }

    /**
     * You should call it when the data center had been switched into the passive mode
     */
    public void suspend(){
        logger.info("WAL completion is suspended");
        suspended.set(true);
        exclusiveLocker.release();
    }

    public boolean isSuspended(){
        return this.suspended.get();
    }

    /**
     * You should call it when the data center had been switched into the active mode
     */
    public void resume(){
        logger.info("WAL completion is resumed");
        this.suspended.set(false);
    }

    public CompletionStatistic completeHangedTransactions() {

        if(suspended.get()){
            logger.info("WAL completion was suspended");
            return new CompletionStatistic(0, 0, 0, 0);
        }

        int staleBatchesCount = 0;
        int completeBatchesCount = 0;
        int ignoredBatchesCount = 0;
        int errorBatchesCount = 0;
        try {
            if(exclusiveLocker.acquire()){
                List<WalRecord<LOCKS, UPDATES, BATCH_ID>> staleBatches = getStaleBatches(staleBatchesThreshold);
                staleBatchesCount += staleBatches.size();
                logger.info("Got {} stale transactions", staleBatches.size());
                for(WalRecord<LOCKS, UPDATES, BATCH_ID> batch : staleBatches){
                    if(suspended.get()){
                        logger.info("WAL completion was suspended");
                        break;
                    }
                    if(Thread.currentThread().isInterrupted()){
                        logger.info("WAL completion was interrupted");
                        break;
                    }

                    if(exclusiveLocker.acquire()) {
                        logger.info("Trying to complete batch batchId=[{}], timestamp=[{}]",
                                batch.batchId, batch.timestamp);
                        try {
                            processAndDeleteTransactions(batch);
                            completeBatchesCount++;
                            logger.info("Successfully complete batch batchId=[{}]", batch.batchId);
                        }
                        //this is expected behaviour that may have place in case of hanged transaction was not completed:
                        //not able to acquire all locks (didn't match expected value
                        // (may have place if initial transaction was interrupted on release stage and released values were modified))
                        catch (LockingException be) {
                            logger.info("Failed to complete batch batchId=[{}] as it's already completed", batch.batchId, be);
                            releaseLocksAndDeleteWalTransactionOnError(batch);
                            ignoredBatchesCount ++;
                            logger.info("released locks for batch batchId=[{}]", batch.batchId, be);
                        }
                        //even in case of error need to move to the next one
                        catch (Exception e) {
                            errorBatchesCount ++;
                            logger.error("!!! Failed to complete batch batchId=[{}], need to be investigated", batch.batchId, e);
                        }
                    }
                }
            }
        }
        catch (Throwable t) {
            logger.error("Error while running completeHangedTransactions()", t);
        }

        return new CompletionStatistic(staleBatchesCount, completeBatchesCount, ignoredBatchesCount, errorBatchesCount);
    }

    abstract protected void releaseLocksAndDeleteWalTransactionOnError(WalRecord<LOCKS, UPDATES, BATCH_ID> batch);

    abstract protected void processAndDeleteTransactions(WalRecord<LOCKS, UPDATES, BATCH_ID> batch);

    abstract protected List<WalRecord<LOCKS, UPDATES, BATCH_ID>> getStaleBatches(Duration staleBatchesThreshold);

}

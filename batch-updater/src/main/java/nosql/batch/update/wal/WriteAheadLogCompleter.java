package nosql.batch.update.wal;

import nosql.batch.update.BatchOperations;
import nosql.batch.update.lock.Lock;
import nosql.batch.update.lock.LockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.util.AsyncUtil.shutdownAndAwaitTermination;

/**
 * Completes hanged transactions
 */
public class WriteAheadLogCompleter<LOCKS, UPDATES, L extends Lock, BATCH_ID> {

    private static Logger logger = LoggerFactory.getLogger(WriteAheadLogCompleter.class);

    private final WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager;
    private final Duration staleBatchesThreshold;
    private final BatchOperations<LOCKS, UPDATES, L, BATCH_ID> batchOperations;

    private final ExclusiveLocker exclusiveLocker;
    private final ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture<?> scheduledFuture;

    private AtomicBoolean suspended = new AtomicBoolean(false);

    /**
     * @param batchOperations
     * @param staleBatchesThreshold
     * @param exclusiveLocker
     * @param scheduledExecutorService
     */
    public WriteAheadLogCompleter(BatchOperations<LOCKS, UPDATES, L, BATCH_ID> batchOperations,
                                  Duration staleBatchesThreshold,
                                  ExclusiveLocker exclusiveLocker, ScheduledExecutorService scheduledExecutorService){
        this.writeAheadLogManager = batchOperations.getWriteAheadLogManager();
        this.batchOperations = batchOperations;
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
        shutdownAndAwaitTermination(scheduledExecutorService);
        exclusiveLocker.release();
        exclusiveLocker.shutdown();
    }

    /**
     * You should call it when the data center had been switched into the passive mode
     */
    public void suspend(){
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
        this.suspended.set(false);
    }

    public CompletionStatistic completeHangedTransactions() {

        if(suspended.get()){
            logger.info("WAL execution was suspended");
            return new CompletionStatistic(0, 0, 0, 0);
        }

        int staleBatchesCount = 0;
        int completeBatchesCount = 0;
        int ignoredBatchesCount = 0;
        int errorBatchesCount = 0;
        try {
            if(exclusiveLocker.acquire()){
                List<WalRecord<LOCKS, UPDATES, BATCH_ID>> staleBatches
                        = writeAheadLogManager.getStaleBatches(staleBatchesThreshold);
                staleBatchesCount += staleBatches.size();
                logger.info("Got {} stale transactions", staleBatches.size());
                for(WalRecord<LOCKS, UPDATES, BATCH_ID> batch : staleBatches){
                    if(suspended.get()){
                        logger.info("WAL execution was suspended");
                        break;
                    }
                    if(Thread.currentThread().isInterrupted()){
                        logger.info("WAL execution was interrupted");
                        break;
                    }

                    if(exclusiveLocker.acquire()) {
                        logger.info("Trying to complete batch batchId=[{}], timestamp=[{}]",
                                batch.batchId, batch.timestamp);
                        LOCKS locks = batch.batchUpdate.locks();
                        try {
                            batchOperations.processAndDeleteTransaction(
                                    batch.batchId, batch.batchUpdate, true).block();
                            completeBatchesCount++;
                            logger.info("Successfully complete batch batchId=[{}]", batch.batchId);
                        }
                        //this is expected behaviour that may have place in case of hanged transaction was not completed:
                        //not able to acquire all locks (didn't match expected value
                        // (may have place if initial transaction was interrupted on release stage and released values were modified))
                        catch (LockingException be) {
                            logger.info("Failed to complete batch batchId=[{}] as it's already completed", batch.batchId, be);
                            batchOperations.releaseLocksAndDeleteWalTransactionOnError(
                                    locks, batch.batchId).block();
                            ignoredBatchesCount ++;
                            logger.info("released locks for batch batchId=[{}]", batch.batchId, be);
                        }
                        //even in case of error need to move to the next one
                        catch (Exception e) {
                            errorBatchesCount ++;
                            logger.error("!!! Failed to complete batch batchId=[{}], need to be investigated",
                                    batch.batchId, e);
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

}

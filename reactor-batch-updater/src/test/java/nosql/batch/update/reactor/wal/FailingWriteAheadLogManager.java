package nosql.batch.update.reactor.wal;

import nosql.batch.update.reactor.BatchUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FailingWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> implements WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> {

    private static Logger logger = LoggerFactory.getLogger(FailingWriteAheadLogManager.class);

    private final WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager;
    private final AtomicBoolean failsDelete;

    private final AtomicInteger deletesInProcess;

    public FailingWriteAheadLogManager(WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager,
                                       AtomicBoolean failsDelete, AtomicInteger deletesInProcess) {
        this.writeAheadLogManager = writeAheadLogManager;
        this.failsDelete = failsDelete;
        this.deletesInProcess = deletesInProcess;
    }

    @Override
    public Mono<BATCH_ID> writeBatch(BatchUpdate<LOCKS, UPDATES> batch) {
        return writeAheadLogManager.writeBatch(batch);
    }

    @Override
    public Mono<Void> deleteBatch(BATCH_ID batchId) {
        if(failsDelete.get()){
            logger.error("deleteBatch failed flaking for batchId [{}]", batchId);
            throw new RuntimeException();
        } else {
            deletesInProcess.incrementAndGet();
            return writeAheadLogManager.deleteBatch(batchId)
                    .doOnSuccess(aVoid -> deletesInProcess.decrementAndGet());
        }
    }

    @Override
    public List<WalRecord<LOCKS, UPDATES, BATCH_ID>> getStaleBatches(Duration staleThreshold) {
        return writeAheadLogManager.getStaleBatches(staleThreshold);
    }
}
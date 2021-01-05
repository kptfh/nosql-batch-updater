package nosql.batch.update.reactor.wal;

import nosql.batch.update.BatchUpdate;
import nosql.batch.update.wal.WalRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ReactorFailingWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> implements ReactorWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> {

    private static final Logger logger = LoggerFactory.getLogger(ReactorFailingWriteAheadLogManager.class);

    private final ReactorWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager;
    private final AtomicBoolean failsDelete;

    private final AtomicInteger deletesInProcess;

    public ReactorFailingWriteAheadLogManager(ReactorWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager,
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
    public Mono<Boolean> deleteBatch(BATCH_ID batchId) {
        if(failsDelete.get()){
            return Mono.defer(() -> {
                logger.error("deleteBatch failed flaking for batchId [{}]", batchId);
                return Mono.<Boolean>error(new RuntimeException())
                        .publishOn(Schedulers.elastic());
            });
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

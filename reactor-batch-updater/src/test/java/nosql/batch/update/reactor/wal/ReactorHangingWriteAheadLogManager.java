package nosql.batch.update.reactor.wal;

import nosql.batch.update.BatchUpdate;
import nosql.batch.update.wal.WalRecord;
import nosql.batch.update.wal.WalTimeRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.reactor.util.ReactorHangingUtil.hang;

public class ReactorHangingWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> implements ReactorWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> {

    private static Logger logger = LoggerFactory.getLogger(ReactorHangingWriteAheadLogManager.class);

    private final ReactorWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager;
    private final AtomicBoolean failsDelete;

    public ReactorHangingWriteAheadLogManager(ReactorWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager, AtomicBoolean failsDelete) {
        this.writeAheadLogManager = writeAheadLogManager;
        this.failsDelete = failsDelete;
    }

    @Override
    public Mono<BATCH_ID> writeBatch(BatchUpdate<LOCKS, UPDATES> batch) {
        return writeAheadLogManager.writeBatch(batch);
    }

    @Override
    public Mono<Boolean> deleteBatch(BATCH_ID batchId) {
        if(failsDelete.get()){
            logger.error("deleteBatch failed hanging for batchId [{}]", batchId);
            return hang();
        } else {
            return writeAheadLogManager.deleteBatch(batchId);
        }
    }

    @Override
    public List<WalTimeRange> getTimeRanges(Duration staleThreshold, int batchSize) {
        return writeAheadLogManager.getTimeRanges(staleThreshold, batchSize);
    }

    @Override
    public List<WalRecord<LOCKS, UPDATES, BATCH_ID>> getStaleBatchesForRange(WalTimeRange timeRange) {
        return writeAheadLogManager.getStaleBatchesForRange(timeRange);
    }

}

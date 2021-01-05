package nosql.batch.update.wal;

import nosql.batch.update.BatchUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.util.HangingUtil.hang;

public class HangingWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> implements WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> {

    private static final Logger logger = LoggerFactory.getLogger(HangingWriteAheadLogManager.class);

    private final WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager;
    private final AtomicBoolean failsDelete;

    public HangingWriteAheadLogManager(WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> writeAheadLogManager, AtomicBoolean failsDelete) {
        this.writeAheadLogManager = writeAheadLogManager;
        this.failsDelete = failsDelete;
    }

    @Override
    public BATCH_ID writeBatch(BatchUpdate<LOCKS, UPDATES> batch) {
        return writeAheadLogManager.writeBatch(batch);
    }

    @Override
    public boolean deleteBatch(BATCH_ID batchId) {
        if(failsDelete.get()){
            logger.error("deleteBatch failed hanging for batchId [{}]", batchId);
            hang();
            throw new IllegalArgumentException();
        } else {
            return writeAheadLogManager.deleteBatch(batchId);
        }
    }

    @Override
    public List<WalRecord<LOCKS, UPDATES, BATCH_ID>> getStaleBatches(Duration staleThreshold) {
        return writeAheadLogManager.getStaleBatches(staleThreshold);
    }
}

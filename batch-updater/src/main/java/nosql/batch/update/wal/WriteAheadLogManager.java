package nosql.batch.update.wal;


import nosql.batch.update.BatchUpdate;

import java.time.Duration;
import java.util.List;

public interface WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> {

    BATCH_ID writeBatch(BatchUpdate<LOCKS, UPDATES> batch);

    boolean deleteBatch(BATCH_ID batchId);

    List<WalTimeRange> getTimeRanges(Duration staleThreshold, int batchSize);

    List<WalRecord<LOCKS, UPDATES, BATCH_ID>> getStaleBatchesForRange(WalTimeRange timeRange);

}

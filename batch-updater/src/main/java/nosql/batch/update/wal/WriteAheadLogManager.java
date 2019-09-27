package nosql.batch.update.wal;


import nosql.batch.update.BatchUpdate;

import java.time.Duration;
import java.util.List;

public interface WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> {

    BATCH_ID writeBatch(BatchUpdate<LOCKS, UPDATES> batch);

    void deleteBatch(BATCH_ID batchId);

    List<WalRecord<LOCKS, UPDATES, BATCH_ID>> getStaleBatches(Duration staleThreshold);

}

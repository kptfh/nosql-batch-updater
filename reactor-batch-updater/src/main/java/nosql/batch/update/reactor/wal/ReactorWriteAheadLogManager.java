package nosql.batch.update.reactor.wal;


import nosql.batch.update.BatchUpdate;
import nosql.batch.update.wal.WalRecord;
import nosql.batch.update.wal.WalTimeRange;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

public interface ReactorWriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> {

    Mono<BATCH_ID> writeBatch(BatchUpdate<LOCKS, UPDATES> batch);

    Mono<Boolean> deleteBatch(BATCH_ID batchId);

    List<WalTimeRange> getTimeRanges(Duration staleThreshold, int batchSize);

    List<WalRecord<LOCKS, UPDATES, BATCH_ID>> getStaleBatchesForRange(WalTimeRange timeRange);

}

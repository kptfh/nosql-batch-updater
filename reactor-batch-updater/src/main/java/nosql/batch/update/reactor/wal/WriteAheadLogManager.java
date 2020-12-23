package nosql.batch.update.reactor.wal;


import nosql.batch.update.reactor.BatchUpdate;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

public interface WriteAheadLogManager<LOCKS, UPDATES, BATCH_ID> {

    Mono<BATCH_ID> writeBatch(BatchUpdate<LOCKS, UPDATES> batch);

    Mono<Void> deleteBatch(BATCH_ID batchId);

    List<WalRecord<LOCKS, UPDATES, BATCH_ID>> getStaleBatches(Duration staleThreshold);

}

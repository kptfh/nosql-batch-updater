package nosql.batch.update.reactor.aerospike.basic.wal;

import com.aerospike.client.Value;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.aerospike.wal.AerospikeExclusiveLocker;
import nosql.batch.update.reactor.ReactorBatchOperations;
import nosql.batch.update.reactor.aerospike.wal.AerospikeReactorWriteAheadLogManager;
import nosql.batch.update.reactor.wal.ReactorWriteAheadLogCompleter;
import nosql.batch.update.reactor.wal.ReactorWriteAheadLogManager;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;

public class AerospikeBasicWalCompleter {

    public static <LOCKS> ReactorWriteAheadLogCompleter<LOCKS, List<Record>, AerospikeLock, Value> basicCompleter(
            ReactorBatchOperations<LOCKS, List<Record>, AerospikeLock, Value> batchOperations,
            Duration staleBatchesThreshold, int batchSize){
        ReactorWriteAheadLogManager<LOCKS, List<Record>, Value> writeAheadLogManager
                = batchOperations.getWriteAheadLogManager();
        AerospikeReactorWriteAheadLogManager aerospikeReactorWriteAheadLogManager = (AerospikeReactorWriteAheadLogManager)writeAheadLogManager;

        return new ReactorWriteAheadLogCompleter<>(
                batchOperations,
                staleBatchesThreshold,
                batchSize,
                new AerospikeExclusiveLocker(
                        aerospikeReactorWriteAheadLogManager.getClient(),
                        aerospikeReactorWriteAheadLogManager.getWalNamespace(),
                        aerospikeReactorWriteAheadLogManager.getWalSetName()),
                Executors.newScheduledThreadPool(1)
        );
    }

}

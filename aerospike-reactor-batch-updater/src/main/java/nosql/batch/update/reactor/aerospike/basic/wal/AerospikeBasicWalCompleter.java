package nosql.batch.update.reactor.aerospike.basic.wal;

import com.aerospike.client.Value;
import nosql.batch.update.reactor.BatchOperations;
import nosql.batch.update.reactor.aerospike.basic.Record;
import nosql.batch.update.reactor.aerospike.lock.AerospikeLock;
import nosql.batch.update.reactor.aerospike.wal.AerospikeExclusiveLocker;
import nosql.batch.update.reactor.aerospike.wal.AerospikeWriteAheadLogManager;
import nosql.batch.update.reactor.wal.WriteAheadLogCompleter;
import nosql.batch.update.reactor.wal.WriteAheadLogManager;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;

public class AerospikeBasicWalCompleter {

    public static <LOCKS> WriteAheadLogCompleter<LOCKS, List<Record>, AerospikeLock, Value> basicCompleter(
            BatchOperations<LOCKS, List<Record>, AerospikeLock, Value> batchOperations,
            Duration staleBatchesThreshold){
        WriteAheadLogManager<LOCKS, List<Record>, Value> writeAheadLogManager
                = batchOperations.getWriteAheadLogManager();
        AerospikeWriteAheadLogManager aerospikeWriteAheadLogManager = (AerospikeWriteAheadLogManager)writeAheadLogManager;

        return new WriteAheadLogCompleter<>(
                batchOperations,
                staleBatchesThreshold,
                new AerospikeExclusiveLocker(
                        aerospikeWriteAheadLogManager.getClient(),
                        aerospikeWriteAheadLogManager.getWalNamespace(),
                        aerospikeWriteAheadLogManager.getWalSetName()),
                Executors.newScheduledThreadPool(1)
        );
    }

}

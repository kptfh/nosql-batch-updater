package nosql.batch.update.aerospike.basic.wal;

import com.aerospike.client.Value;
import nosql.batch.update.BatchOperations;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.aerospike.wal.AerospikeExclusiveLocker;
import nosql.batch.update.aerospike.wal.AerospikeWriteAheadLogManager;
import nosql.batch.update.wal.WriteAheadLogCompleter;
import nosql.batch.update.wal.WriteAheadLogManager;

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

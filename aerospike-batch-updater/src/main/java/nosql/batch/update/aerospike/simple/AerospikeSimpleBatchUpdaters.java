package nosql.batch.update.aerospike.simple;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import nosql.batch.update.BatchOperations;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.aerospike.lock.AerospikeLockOperations;
import nosql.batch.update.aerospike.simple.lock.AerospikeSimpleBatchLocks;
import nosql.batch.update.aerospike.wal.AerospikeExclusiveLocker;
import nosql.batch.update.aerospike.wal.AerospikeWriteAheadLogManager;
import nosql.batch.update.wal.WriteAheadLogCompleter;
import nosql.batch.update.wal.WriteAheadLogManager;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AerospikeSimpleBatchUpdaters {

    public static BatchOperations<AerospikeSimpleBatchLocks, List<Record>, AerospikeLock, Value> postLockOperations(
            IAerospikeClient client,
            String walNamespace,
            String walSetName,
            Clock clock,
            ExecutorService executorService){

        AerospikeWriteAheadLogManager<AerospikeSimpleBatchLocks, List<Record>, List<Record>> walManager =
                new AerospikeWriteAheadLogManager<>(
                        client, walNamespace, walSetName,
                        new AerospikeSimpleBatchUpdateSerde(),
                        clock);

        AerospikeLockOperations<AerospikeSimpleBatchLocks, List<Record>> lockOperations = new AerospikeLockOperations<>(
                client,
                new AerospikeSimpleExpectedValueOperations(client),
                executorService);

        AerospikeSimpleUpdateOperations updateOperations = new AerospikeSimpleUpdateOperations(client);

        return new BatchOperations<>(walManager, lockOperations, updateOperations);
    }

    public static WriteAheadLogCompleter<AerospikeSimpleBatchLocks, List<Record>, AerospikeLock, Value> postLockCompleter(
            BatchOperations<AerospikeSimpleBatchLocks, List<Record>, AerospikeLock, Value> batchOperations,
            Duration staleBatchesThreshold){
        WriteAheadLogManager<AerospikeSimpleBatchLocks, List<Record>, Value> writeAheadLogManager
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

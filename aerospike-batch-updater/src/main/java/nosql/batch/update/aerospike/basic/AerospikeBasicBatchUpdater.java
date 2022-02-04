package nosql.batch.update.aerospike.basic;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import nosql.batch.update.BatchOperations;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.aerospike.lock.AerospikeLockOperations;
import nosql.batch.update.aerospike.wal.AerospikeWriteAheadLogManager;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class AerospikeBasicBatchUpdater {

    public static BatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> basicOperations(
            IAerospikeClient client,
            String walNamespace,
            String walSetName,
            Clock clock,
            ExecutorService aerospikeExecutorService){

        AerospikeWriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, List<Record>> walManager =
                basicWalManager(client, walNamespace, walSetName, clock);

        AerospikeLockOperations<AerospikeBasicBatchLocks, List<Record>> lockOperations =
                basicLockOperations(client, aerospikeExecutorService);

        AerospikeBasicUpdateOperations updateOperations = basicUpdateOperations(client, aerospikeExecutorService);

        return new BatchOperations<>(walManager, lockOperations, updateOperations, aerospikeExecutorService);
    }

    public static AerospikeBasicUpdateOperations basicUpdateOperations(
            IAerospikeClient client, ExecutorService executorService) {
        return new AerospikeBasicUpdateOperations(client, executorService);
    }

    public static AerospikeLockOperations<AerospikeBasicBatchLocks, List<Record>> basicLockOperations(
            IAerospikeClient reactorClient,
            ExecutorService aerospikeExecutorService) {
        return new AerospikeLockOperations<>(
                reactorClient,
                new AerospikeBasicExpectedValueOperations(reactorClient),
                aerospikeExecutorService);
    }

    public static AerospikeWriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, List<Record>> basicWalManager(
            IAerospikeClient client, String walNamespace, String walSetName, Clock clock) {
        return new AerospikeWriteAheadLogManager<>(
                client, walNamespace, walSetName,
                new AerospikeBasicBatchUpdateSerde(), clock);
    }

}

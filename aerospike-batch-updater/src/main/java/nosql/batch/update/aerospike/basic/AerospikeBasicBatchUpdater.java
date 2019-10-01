package nosql.batch.update.aerospike.basic;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.reactor.IAerospikeReactorClient;
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
            IAerospikeClient client, IAerospikeReactorClient reactorClient,
            String walNamespace,
            String walSetName,
            Clock clock,
            ExecutorService executorService){

        AerospikeWriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, List<Record>> walManager =
                basicWalManager(client, reactorClient, walNamespace, walSetName, clock);

        AerospikeLockOperations<AerospikeBasicBatchLocks, List<Record>> lockOperations =
                basicLockOperations(client, reactorClient, executorService);

        AerospikeBasicUpdateOperations updateOperations = basicUpdateOperations(client);

        return new BatchOperations<>(walManager, lockOperations, updateOperations);
    }

    public static AerospikeBasicUpdateOperations basicUpdateOperations(IAerospikeClient client) {
        return new AerospikeBasicUpdateOperations(client);
    }

    public static AerospikeLockOperations<AerospikeBasicBatchLocks, List<Record>> basicLockOperations(
            IAerospikeClient client, IAerospikeReactorClient reactorClient, ExecutorService executorService) {
        return new AerospikeLockOperations<>(
                client, reactorClient,
                new AerospikeBasicExpectedValueOperations(client),
                executorService);
    }

    public static AerospikeWriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, List<Record>> basicWalManager(
            IAerospikeClient client, IAerospikeReactorClient reactorClient, String walNamespace, String walSetName, Clock clock) {
        return new AerospikeWriteAheadLogManager<>(
                client, reactorClient, walNamespace, walSetName,
                new AerospikeBasicBatchUpdateSerde(),
                clock);
    }

}

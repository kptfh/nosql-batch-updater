package nosql.batch.update.reactor.aerospike.basic;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.reactor.BatchOperations;
import nosql.batch.update.reactor.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.reactor.aerospike.lock.AerospikeLock;
import nosql.batch.update.reactor.aerospike.lock.AerospikeLockOperations;
import nosql.batch.update.reactor.aerospike.wal.AerospikeWriteAheadLogManager;

import java.time.Clock;
import java.util.List;

public class AerospikeBasicBatchUpdater {

    public static BatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> basicOperations(
            IAerospikeClient client, IAerospikeReactorClient reactorClient,
            String walNamespace,
            String walSetName,
            Clock clock){

        AerospikeWriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, List<Record>> walManager =
                basicWalManager(client, reactorClient, walNamespace, walSetName, clock);

        AerospikeLockOperations<AerospikeBasicBatchLocks, List<Record>> lockOperations =
                basicLockOperations(reactorClient);

        AerospikeBasicUpdateOperations updateOperations = basicUpdateOperations(reactorClient);

        return new BatchOperations<>(walManager, lockOperations, updateOperations);
    }

    public static AerospikeBasicUpdateOperations basicUpdateOperations(IAerospikeReactorClient client) {
        return new AerospikeBasicUpdateOperations(client);
    }

    public static AerospikeLockOperations<AerospikeBasicBatchLocks, List<Record>> basicLockOperations(
            IAerospikeReactorClient reactorClient) {
        return new AerospikeLockOperations<>(
                reactorClient,
                new AerospikeBasicExpectedValueOperations(reactorClient));
    }

    public static AerospikeWriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, List<Record>> basicWalManager(
            IAerospikeClient client, IAerospikeReactorClient reactorClient, String walNamespace, String walSetName, Clock clock) {
        return new AerospikeWriteAheadLogManager<>(
                client, reactorClient, walNamespace, walSetName,
                new AerospikeBasicBatchUpdateSerde(),
                clock);
    }

}

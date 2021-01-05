package nosql.batch.update.reactor.aerospike.basic;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.aerospike.basic.AerospikeBasicBatchUpdateSerde;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.reactor.ReactorBatchOperations;
import nosql.batch.update.reactor.aerospike.lock.AerospikeReactorLockOperations;
import nosql.batch.update.reactor.aerospike.wal.AerospikeReactorWriteAheadLogManager;

import java.time.Clock;
import java.util.List;

public class AerospikeBasicBatchUpdater {

    public static ReactorBatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> basicOperations(
            IAerospikeClient client, IAerospikeReactorClient reactorClient,
            String walNamespace,
            String walSetName,
            Clock clock){

        AerospikeReactorWriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, List<Record>> walManager =
                basicWalManager(client, reactorClient, walNamespace, walSetName, clock);

        AerospikeReactorLockOperations<AerospikeBasicBatchLocks, List<Record>> lockOperations =
                basicLockOperations(reactorClient);

        AerospikeBasicReactorUpdateOperations updateOperations = basicUpdateOperations(reactorClient);

        return new ReactorBatchOperations<>(walManager, lockOperations, updateOperations);
    }

    public static AerospikeBasicReactorUpdateOperations basicUpdateOperations(IAerospikeReactorClient client) {
        return new AerospikeBasicReactorUpdateOperations(client);
    }

    public static AerospikeReactorLockOperations<AerospikeBasicBatchLocks, List<Record>> basicLockOperations(
            IAerospikeReactorClient reactorClient) {
        return new AerospikeReactorLockOperations<>(
                reactorClient,
                new AerospikeBasicReactorExpectedValueOperations(reactorClient));
    }

    public static AerospikeReactorWriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, List<Record>> basicWalManager(
            IAerospikeClient client, IAerospikeReactorClient reactorClient, String walNamespace, String walSetName, Clock clock) {
        return new AerospikeReactorWriteAheadLogManager<>(
                client, reactorClient, walNamespace, walSetName,
                new AerospikeBasicBatchUpdateSerde(),
                clock);
    }

}

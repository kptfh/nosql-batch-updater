package nosql.batch.update.reactor.aerospike.basic.util;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.reactor.ReactorBatchOperations;

import java.time.Clock;
import java.util.List;

import static nosql.batch.update.reactor.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.reactor.aerospike.basic.AerospikeBasicBatchUpdater.basicOperations;

public class BasicOperationsUtil {

    public static ReactorBatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> getBasicOperations(
            AerospikeClient client, IAerospikeReactorClient reactorClient, Clock clock) {
        return basicOperations(
                client, reactorClient,
                AEROSPIKE_PROPERTIES.getNamespace(), "wal",
                clock);
    }

}

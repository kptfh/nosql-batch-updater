package nosql.batch.update.aerospike.basic.util;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.BatchOperations;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;

import java.time.Clock;
import java.util.List;

import static nosql.batch.update.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.aerospike.basic.AerospikeBasicBatchUpdater.basicOperations;

public class BasicOperationsUtil {

    public static BatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> getBasicOperations(
            AerospikeClient client, IAerospikeReactorClient reactorClient, Clock clock) {
        return basicOperations(
                client, reactorClient,
                AEROSPIKE_PROPERTIES.getNamespace(), "wal",
                clock);
    }

}

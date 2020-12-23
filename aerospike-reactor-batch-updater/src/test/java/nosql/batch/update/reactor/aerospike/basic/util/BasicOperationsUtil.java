package nosql.batch.update.reactor.aerospike.basic.util;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.reactor.BatchOperations;
import nosql.batch.update.reactor.aerospike.basic.Record;
import nosql.batch.update.reactor.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.reactor.aerospike.lock.AerospikeLock;

import java.time.Clock;
import java.util.List;

import static nosql.batch.update.reactor.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.reactor.aerospike.basic.AerospikeBasicBatchUpdater.basicOperations;

public class BasicOperationsUtil {

    public static BatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> getBasicOperations(
            AerospikeClient client, IAerospikeReactorClient reactorClient, Clock clock) {
        return basicOperations(
                client, reactorClient,
                AEROSPIKE_PROPERTIES.getNamespace(), "wal",
                clock);
    }

}

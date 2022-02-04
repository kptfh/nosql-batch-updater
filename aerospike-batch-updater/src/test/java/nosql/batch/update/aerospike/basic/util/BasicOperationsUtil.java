package nosql.batch.update.aerospike.basic.util;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import nosql.batch.update.BatchOperations;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static nosql.batch.update.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.aerospike.basic.AerospikeBasicBatchUpdater.basicOperations;

public class BasicOperationsUtil {

    public static BatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> getBasicOperations(
            AerospikeClient client,
            Clock clock,
            ExecutorService aerospikeExecutorService,
            ExecutorService batchExecutorService) {
        return basicOperations(
                client,
                AEROSPIKE_PROPERTIES.getNamespace(), "wal",
                clock,
                aerospikeExecutorService,
                batchExecutorService);
    }

}

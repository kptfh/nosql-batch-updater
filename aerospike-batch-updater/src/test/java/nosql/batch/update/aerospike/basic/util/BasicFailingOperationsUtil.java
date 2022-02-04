package nosql.batch.update.aerospike.basic.util;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import nosql.batch.update.BatchOperations;
import nosql.batch.update.UpdateOperations;
import nosql.batch.update.aerospike.basic.AerospikeBasicExpectedValueOperations;
import nosql.batch.update.aerospike.basic.AerospikeBasicUpdateOperations;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicFailingLockOperations;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.lock.LockOperations;
import nosql.batch.update.wal.WriteAheadLogManager;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static nosql.batch.update.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.aerospike.basic.AerospikeBasicBatchUpdater.basicWalManager;
import static nosql.batch.update.aerospike.basic.AerospikeBasicFailingUpdateOperations.failingUpdates;
import static nosql.batch.update.aerospike.wal.AerospikeFailingWriteAheadLogManager.failingWal;

public class BasicFailingOperationsUtil {

    public static BatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> failingOperations(
            IAerospikeClient client,
            Clock clock,
            ExecutorService executorService,
            AtomicReference<Throwable> failsAcquire,
            AtomicReference<Throwable> failsCheckValue,
            AtomicBoolean failsUpdate,
            AtomicBoolean failsRelease,
            AtomicBoolean failsDeleteWal,
            AtomicInteger deletesInProcess){

        LockOperations<AerospikeBasicBatchLocks, AerospikeLock, Value> lockOperations
                = new AerospikeBasicFailingLockOperations(client, executorService,
                new AerospikeBasicExpectedValueOperations(client),
                failsAcquire, failsCheckValue, failsRelease);

        UpdateOperations<List<Record>> updateOperations =
                failingUpdates(new AerospikeBasicUpdateOperations(client, executorService), failsUpdate);

        WriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, Value> walManager
                = failingWal(basicWalManager(client, AEROSPIKE_PROPERTIES.getNamespace(), "wal", clock),
                failsDeleteWal, deletesInProcess);

        return new BatchOperations<>(walManager, lockOperations, updateOperations, executorService);
    }

}

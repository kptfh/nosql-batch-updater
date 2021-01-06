package nosql.batch.update.aerospike.basic.util;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import nosql.batch.update.BatchOperations;
import nosql.batch.update.UpdateOperations;
import nosql.batch.update.aerospike.basic.AerospikeBasicUpdateOperations;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.lock.LockOperations;
import nosql.batch.update.wal.WriteAheadLogManager;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.aerospike.basic.AerospikeBasicBatchUpdater.basicLockOperations;
import static nosql.batch.update.aerospike.basic.AerospikeBasicBatchUpdater.basicWalManager;
import static nosql.batch.update.aerospike.basic.AerospikeBasicHangingUpdateOperations.hangingUpdates;
import static nosql.batch.update.aerospike.basic.lock.AerospikeBasicHangingLockOperations.hangingLocks;
import static nosql.batch.update.aerospike.wal.AerospikeHangingWriteAheadLogManager.hangingWal;

public class BasicHangingOperationsUtil {

    public static BatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> hangingOperations(
            IAerospikeClient client,
            ExecutorService executorService,
            Clock clock,
            AtomicBoolean hangsAcquire,
            AtomicBoolean hangsUpdate,
            AtomicBoolean hangsRelease,
            AtomicBoolean hangsDeleteWal){

        LockOperations<AerospikeBasicBatchLocks, AerospikeLock, Value> lockOperations
                = hangingLocks(basicLockOperations(client, executorService),
                hangsAcquire, hangsRelease);

        UpdateOperations<List<Record>> updateOperations =
                hangingUpdates(new AerospikeBasicUpdateOperations(client, executorService), hangsUpdate);

        WriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, Value> walManager
                = hangingWal(basicWalManager(client, AEROSPIKE_PROPERTIES.getNamespace(), "wal", clock,
                Executors.newCachedThreadPool()),
                hangsDeleteWal);

        return new BatchOperations<>(walManager, lockOperations, updateOperations, executorService);
    }

}

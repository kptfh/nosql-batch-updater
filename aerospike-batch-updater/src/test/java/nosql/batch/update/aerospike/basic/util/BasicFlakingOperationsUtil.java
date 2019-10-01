package nosql.batch.update.aerospike.basic.util;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.reactor.IAerospikeReactorClient;
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
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.aerospike.basic.AerospikeBasicBatchUpdater.basicLockOperations;
import static nosql.batch.update.aerospike.basic.AerospikeBasicBatchUpdater.basicWalManager;
import static nosql.batch.update.aerospike.basic.AerospikeBasicFlakingUpdateOperations.flakingUpdates;
import static nosql.batch.update.aerospike.basic.lock.AerospikeBasicFlakingLockOperations.flakingLocks;
import static nosql.batch.update.aerospike.wal.AerospikeFlakingWriteAheadLogManager.flakingWal;

public class BasicFlakingOperationsUtil {

    public static BatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> flakingOperations(
            IAerospikeClient client, IAerospikeReactorClient reactorClient,
            Clock clock,
            AtomicBoolean failsAcquire,
            AtomicBoolean failsUpdate,
            AtomicBoolean failsRelease,
            AtomicBoolean failsDeleteWal){

        LockOperations<AerospikeBasicBatchLocks, AerospikeLock, Value> lockOperations
                = flakingLocks(basicLockOperations(client, reactorClient, Executors.newFixedThreadPool(4)),
                failsAcquire, failsRelease);

        UpdateOperations<List<Record>> updateOperations =
                flakingUpdates(new AerospikeBasicUpdateOperations(client), failsUpdate);

        WriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, Value> walManager
                = flakingWal(basicWalManager(client, reactorClient, AEROSPIKE_PROPERTIES.getNamespace(), "wal", clock),
                failsDeleteWal);

        return new BatchOperations<>(walManager, lockOperations, updateOperations);
    }

}

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
import static nosql.batch.update.aerospike.basic.AerospikeBasicHangingUpdateOperations.hangingUpdates;
import static nosql.batch.update.aerospike.basic.lock.AerospikeBasicHangingLockOperations.hangingLocks;
import static nosql.batch.update.aerospike.wal.AerospikeHangingWriteAheadLogManager.hangingWal;

public class BasicHangingOperationsUtil {

    public static BatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> hangingOperations(
            IAerospikeClient client, IAerospikeReactorClient reactorClient,
            Clock clock,
            AtomicBoolean hangsAcquire,
            AtomicBoolean hangsUpdate,
            AtomicBoolean hangsRelease,
            AtomicBoolean hangsDeleteWal){

        LockOperations<AerospikeBasicBatchLocks, AerospikeLock, Value> lockOperations
                = hangingLocks(basicLockOperations(client, reactorClient, Executors.newFixedThreadPool(4)),
                hangsAcquire, hangsRelease);

        UpdateOperations<List<Record>> updateOperations =
                hangingUpdates(new AerospikeBasicUpdateOperations(client), hangsUpdate);

        WriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, Value> walManager
                = hangingWal(basicWalManager(client, reactorClient, AEROSPIKE_PROPERTIES.getNamespace(), "wal", clock),
                hangsDeleteWal);

        return new BatchOperations<>(walManager, lockOperations, updateOperations);
    }

}

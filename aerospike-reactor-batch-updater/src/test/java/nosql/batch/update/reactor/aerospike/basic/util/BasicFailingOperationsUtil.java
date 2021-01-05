package nosql.batch.update.reactor.aerospike.basic.util;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.reactor.ReactorBatchOperations;
import nosql.batch.update.reactor.ReactorUpdateOperations;
import nosql.batch.update.reactor.aerospike.basic.AerospikeBasicReactorExpectedValueOperations;
import nosql.batch.update.reactor.aerospike.basic.AerospikeBasicReactorUpdateOperations;
import nosql.batch.update.reactor.aerospike.basic.lock.AerospikeBasicFailingReactorLockOperations;
import nosql.batch.update.reactor.lock.ReactorLockOperations;
import nosql.batch.update.reactor.wal.ReactorWriteAheadLogManager;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static nosql.batch.update.reactor.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.reactor.aerospike.basic.AerospikeBasicBatchUpdater.basicWalManager;
import static nosql.batch.update.reactor.aerospike.basic.AerospikeBasicFailingUpdateOperations.failingUpdates;
import static nosql.batch.update.reactor.aerospike.wal.AerospikeFailingWriteAheadLogManager.failingWal;

public class BasicFailingOperationsUtil {

    public static ReactorBatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> failingOperations(
            IAerospikeClient client, IAerospikeReactorClient reactorClient,
            Clock clock,
            AtomicBoolean failsAcquire,
            AtomicBoolean failsCheckValue,
            AtomicBoolean failsUpdate,
            AtomicBoolean failsRelease,
            AtomicBoolean failsDeleteWal,
            AtomicInteger deletesInProcess){

        ReactorLockOperations<AerospikeBasicBatchLocks, AerospikeLock, Value> lockOperations
                = new AerospikeBasicFailingReactorLockOperations(reactorClient,
                new AerospikeBasicReactorExpectedValueOperations(reactorClient),
                failsAcquire, failsCheckValue, failsRelease);

        ReactorUpdateOperations<List<Record>> updateOperations =
                failingUpdates(new AerospikeBasicReactorUpdateOperations(reactorClient), failsUpdate);

        ReactorWriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, Value> walManager
                = failingWal(basicWalManager(client, reactorClient, AEROSPIKE_PROPERTIES.getNamespace(), "wal", clock),
                failsDeleteWal, deletesInProcess);

        return new ReactorBatchOperations<>(walManager, lockOperations, updateOperations);
    }

}

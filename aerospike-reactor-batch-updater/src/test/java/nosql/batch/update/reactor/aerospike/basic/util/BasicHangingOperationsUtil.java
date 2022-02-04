package nosql.batch.update.reactor.aerospike.basic.util;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.reactor.ReactorBatchOperations;
import nosql.batch.update.reactor.ReactorUpdateOperations;
import nosql.batch.update.reactor.aerospike.basic.AerospikeBasicReactorUpdateOperations;
import nosql.batch.update.reactor.lock.ReactorLockOperations;
import nosql.batch.update.reactor.wal.ReactorWriteAheadLogManager;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.reactor.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.reactor.aerospike.basic.AerospikeBasicBatchUpdater.basicLockOperations;
import static nosql.batch.update.reactor.aerospike.basic.AerospikeBasicBatchUpdater.basicWalManager;
import static nosql.batch.update.reactor.aerospike.basic.AerospikeBasicHangingUpdateOperations.hangingUpdates;
import static nosql.batch.update.reactor.aerospike.basic.lock.AerospikeBasicHangingLockOperations.hangingLocks;
import static nosql.batch.update.reactor.aerospike.wal.AerospikeHangingWriteAheadLogManager.hangingWal;

public class BasicHangingOperationsUtil {

    public static ReactorBatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> hangingOperations(
            IAerospikeClient client, IAerospikeReactorClient reactorClient,
            Clock clock,
            AtomicBoolean hangsAcquire,
            AtomicBoolean hangsUpdate,
            AtomicBoolean hangsRelease,
            AtomicBoolean hangsDeleteWal){

        ReactorLockOperations<AerospikeBasicBatchLocks, AerospikeLock, Value> lockOperations
                = hangingLocks(basicLockOperations(reactorClient),
                hangsAcquire, hangsRelease);

        ReactorUpdateOperations<List<Record>> updateOperations =
                hangingUpdates(new AerospikeBasicReactorUpdateOperations(reactorClient), hangsUpdate);

        ReactorWriteAheadLogManager<AerospikeBasicBatchLocks, List<Record>, Value> walManager
                = hangingWal(basicWalManager(client, reactorClient, AEROSPIKE_PROPERTIES.getNamespace(), "wal", clock),
                hangsDeleteWal);

        return new ReactorBatchOperations<>(walManager, lockOperations, updateOperations);
    }

}

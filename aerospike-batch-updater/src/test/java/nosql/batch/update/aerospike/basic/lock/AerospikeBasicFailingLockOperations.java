package nosql.batch.update.aerospike.basic.lock;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.lock.AerospikeExpectedValuesOperations;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.aerospike.lock.AerospikeLockOperations;
import nosql.batch.update.lock.LockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static nosql.batch.update.util.HangingUtil.selectFlaking;

public class AerospikeBasicFailingLockOperations
        extends AerospikeLockOperations<AerospikeBasicBatchLocks, List<Record>> {

    private static Logger logger = LoggerFactory.getLogger(AerospikeBasicFailingLockOperations.class);

    private final AtomicBoolean failsAcquire;
    private final AtomicBoolean failsCheckValue;
    private final AtomicBoolean failsRelease;

    public AerospikeBasicFailingLockOperations(IAerospikeClient client,
                                               IAerospikeReactorClient reactorClient,
                                               AerospikeExpectedValuesOperations<List<Record>> expectedValuesOperations,
                                               ExecutorService executorService,
                                               AtomicBoolean failsAcquire,
                                               AtomicBoolean failsCheckValue,
                                               AtomicBoolean failsRelease) {
        super(client, reactorClient, expectedValuesOperations, executorService);
        this.failsAcquire = failsAcquire;
        this.failsCheckValue = failsCheckValue;
        this.failsRelease = failsRelease;
    }

    @Override
    protected List<AerospikeLock> putLocks(
            Value batchId,
            AerospikeBasicBatchLocks batchLocks,
            boolean checkTransactionId,
            Consumer<Collection<AerospikeLock>> onErrorCleanup) throws LockingException {
        if(failsAcquire.get()){
            List<Record> recordsSelected = selectFlaking(batchLocks.expectedValues(),
                    key -> logger.info("acquire locks failed flaking for key [{}]", key));

            return super.putLocks(batchId,
                    new AerospikeBasicBatchLocks(recordsSelected),
                    checkTransactionId, onErrorCleanup);
        } else {
            return super.putLocks(batchId, batchLocks, checkTransactionId, onErrorCleanup);
        }
    }

    @Override
    protected void checkExpectedValues(AerospikeBasicBatchLocks batchLocks, List<AerospikeLock> keysLocked) {
        if(failsCheckValue.get()){
            throw new RuntimeException();
        } else {
            super.checkExpectedValues(batchLocks, keysLocked);
        }
    }

    @Override
    public Mono<Void> release(Collection<AerospikeLock> locks, Value batchId) {
        if(failsRelease.get()){
            Collection<AerospikeLock> partialLocks = selectFlaking(locks,
                    key -> logger.info("release locks failed flaking for key [{}]", key));
            return super.release(partialLocks, batchId)
                    .then(Mono.error(new RuntimeException()));
        } else {
            return super.release(locks, batchId);
        }
    }

}

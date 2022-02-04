package nosql.batch.update.reactor.aerospike.basic.lock;

import com.aerospike.client.Value;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.reactor.aerospike.lock.AerospikeReactorExpectedValuesOperations;
import nosql.batch.update.reactor.aerospike.lock.AerospikeReactorLockOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static nosql.batch.update.util.HangingUtil.selectFlaking;


public class AerospikeBasicFailingReactorLockOperations
        extends AerospikeReactorLockOperations<AerospikeBasicBatchLocks, List<Record>> {

    private static final Logger logger = LoggerFactory.getLogger(AerospikeBasicFailingReactorLockOperations.class);

    private final AtomicReference<Throwable> failsAcquire;
    private final AtomicReference<Throwable> failsCheckValue;
    private final AtomicBoolean failsRelease;

    public AerospikeBasicFailingReactorLockOperations(IAerospikeReactorClient reactorClient,
                                                      AerospikeReactorExpectedValuesOperations<List<Record>> expectedValuesOperations,
                                                      AtomicReference<Throwable> failsAcquire,
                                                      AtomicReference<Throwable> failsCheckValue,
                                                      AtomicBoolean failsRelease) {
        super(reactorClient, expectedValuesOperations);
        this.failsAcquire = failsAcquire;
        this.failsCheckValue = failsCheckValue;
        this.failsRelease = failsRelease;
    }

    @Override
    protected Mono<List<AerospikeLock>> putLocks(
            Value batchId,
            AerospikeBasicBatchLocks batchLocks,
            boolean checkTransactionId) {
        Throwable throwable = failsAcquire.get();
        if(throwable != null){
            List<Record> recordsSelected = selectFlaking(batchLocks.expectedValues(),
                    key -> logger.info("acquire locks failed flaking for key [{}]", key));

            return super.putLocks(batchId,
                    new AerospikeBasicBatchLocks(recordsSelected),
                    checkTransactionId)
                    .then(Mono.error(Exceptions.propagate(throwable)));
        } else {
            return super.putLocks(batchId, batchLocks, checkTransactionId);
        }
    }

    @Override
    protected Mono<Void> checkExpectedValues(AerospikeBasicBatchLocks batchLocks, List<AerospikeLock> keysLocked) {
        Throwable throwable = failsCheckValue.get();
        if(throwable != null){
            return Mono.error(throwable);
        } else {
            return super.checkExpectedValues(batchLocks, keysLocked);
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
